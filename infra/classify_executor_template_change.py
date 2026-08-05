"""Classify direct ExecutorHost rollout effects between WorkerStack templates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

_TASK_PATH_SUFFIX = "/ExecutorHostTaskDef/Resource"
_SERVICE_PATH_SUFFIX = "/ExecutorHostService/Service"
_TASK_TYPE = "AWS::ECS::TaskDefinition"
_SERVICE_TYPE = "AWS::ECS::Service"
_SERVICE_REDEPLOY_PROPERTIES = frozenset(
    {
        "Cluster",
        "LaunchType",
        "LoadBalancers",
        "NetworkConfiguration",
        "PlatformVersion",
        "Role",
        "SchedulingStrategy",
        "ServiceConnectConfiguration",
        "ServiceName",
        "ServiceRegistries",
        "TaskDefinition",
        "VolumeConfigurations",
        "VpcLatticeConfigurations",
    }
)
_SERVICE_NON_REDEPLOY_PROPERTIES = frozenset(
    {
        "AvailabilityZoneRebalancing",
        "CapacityProviderStrategy",
        "DeploymentConfiguration",
        "DesiredCount",
        "EnableECSManagedTags",
        "EnableExecuteCommand",
        "HealthCheckGracePeriodSeconds",
        "PlacementConstraints",
        "PlacementStrategies",
        "PropagateTags",
        "Tags",
    }
)


class TemplateClassificationError(ValueError):
    """Raised when template input cannot prove a maintenance decision."""


@dataclass(frozen=True)
class ExecutorHostTemplateEffect:
    redeploy_required: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class _HostResource:
    logical_id: str
    resource_type: str
    properties: Mapping[str, object]


@dataclass(frozen=True)
class _HostResources:
    task_definition: _HostResource | None
    service: _HostResource | None

    @property
    def present(self) -> bool:
        return self.task_definition is not None


def classify_executor_host_template_change(
    base_template: Mapping[str, object],
    head_template: Mapping[str, object],
    *,
    expected_stack_id: str,
) -> ExecutorHostTemplateEffect:
    """Return direct ExecutorHost task/service rollout effects."""
    if not expected_stack_id or "/" in expected_stack_id:
        raise TemplateClassificationError("expected_stack_id must be one non-empty CDK stack ID")

    base = _host_resources(base_template, expected_stack_id=expected_stack_id, revision="base")
    head = _host_resources(head_template, expected_stack_id=expected_stack_id, revision="head")

    if not base.present:
        if head.present:
            _require_expected_types(head, revision="head")
        return ExecutorHostTemplateEffect(redeploy_required=False, reasons=())

    _require_expected_types(base, revision="base")
    if not head.present:
        return ExecutorHostTemplateEffect(redeploy_required=True, reasons=("executor-host-removed",))

    assert base.task_definition is not None
    assert base.service is not None
    assert head.task_definition is not None
    assert head.service is not None

    reasons: set[str] = set()
    if _resource_identity_changed(base.task_definition, head.task_definition):
        reasons.add("executor-host-task-definition-replaced")
    elif _task_properties(base.task_definition) != _task_properties(head.task_definition):
        reasons.add("executor-host-task-definition-changed")

    if _resource_identity_changed(base.service, head.service):
        reasons.add("executor-host-service-replaced")
    else:
        reasons.update(_service_change_reasons(base.service, head.service))

    return ExecutorHostTemplateEffect(redeploy_required=bool(reasons), reasons=tuple(sorted(reasons)))


def _host_resources(
    template: Mapping[str, object],
    *,
    expected_stack_id: str,
    revision: str,
) -> _HostResources:
    raw_resources = template.get("Resources")
    if not isinstance(raw_resources, Mapping):
        raise TemplateClassificationError(f"{revision} template must contain an object-valued Resources field")
    resources = cast(Mapping[object, object], raw_resources)

    task_path = f"{expected_stack_id}{_TASK_PATH_SUFFIX}"
    service_path = f"{expected_stack_id}{_SERVICE_PATH_SUFFIX}"
    task_matches: list[_HostResource] = []
    service_matches: list[_HostResource] = []
    foreign_host_paths: list[str] = []

    for logical_id, raw_resource in resources.items():
        if not isinstance(logical_id, str) or not isinstance(raw_resource, Mapping):
            raise TemplateClassificationError(f"{revision} template resources must be named objects")
        resource = cast(Mapping[str, object], raw_resource)
        raw_metadata = resource.get("Metadata", {})
        if not isinstance(raw_metadata, Mapping):
            raise TemplateClassificationError(f"{revision} resource {logical_id} Metadata must be an object")
        metadata = cast(Mapping[str, object], raw_metadata)
        cdk_path = metadata.get("aws:cdk:path")
        if not isinstance(cdk_path, str):
            continue
        if cdk_path == task_path:
            task_matches.append(_resource(logical_id, resource, revision=revision))
        elif cdk_path == service_path:
            service_matches.append(_resource(logical_id, resource, revision=revision))
        elif cdk_path.endswith((_TASK_PATH_SUFFIX, _SERVICE_PATH_SUFFIX)):
            foreign_host_paths.append(cdk_path)

    if foreign_host_paths:
        rendered = ", ".join(sorted(foreign_host_paths))
        raise TemplateClassificationError(
            f"{revision} template contains ExecutorHost resources for another stack: {rendered}"
        )
    if len(task_matches) > 1 or len(service_matches) > 1:
        raise TemplateClassificationError(f"{revision} template contains duplicate canonical ExecutorHost resources")
    if bool(task_matches) != bool(service_matches):
        raise TemplateClassificationError(
            f"{revision} template must contain both ExecutorHost task definition and service, or neither"
        )

    task = task_matches[0] if task_matches else None
    service = service_matches[0] if service_matches else None
    return _HostResources(task_definition=task, service=service)


def _resource(logical_id: str, resource: Mapping[str, object], *, revision: str) -> _HostResource:
    resource_type = resource.get("Type")
    properties = resource.get("Properties")
    if not isinstance(resource_type, str) or not isinstance(properties, Mapping):
        raise TemplateClassificationError(
            f"{revision} resource {logical_id} must have string Type and object Properties"
        )
    return _HostResource(
        logical_id=logical_id,
        resource_type=resource_type,
        properties=cast(Mapping[str, object], properties),
    )


def _require_expected_types(resources: _HostResources, *, revision: str) -> None:
    assert resources.task_definition is not None
    assert resources.service is not None
    if resources.task_definition.resource_type != _TASK_TYPE:
        raise TemplateClassificationError(
            f"{revision} ExecutorHost task resource has unexpected type {resources.task_definition.resource_type!r}"
        )
    if resources.service.resource_type != _SERVICE_TYPE:
        raise TemplateClassificationError(
            f"{revision} ExecutorHost service resource has unexpected type {resources.service.resource_type!r}"
        )


def _resource_identity_changed(base: _HostResource, head: _HostResource) -> bool:
    return base.logical_id != head.logical_id or base.resource_type != head.resource_type


def _task_properties(resource: _HostResource) -> dict[str, object]:
    return {key: value for key, value in resource.properties.items() if key != "Tags"}


def _service_change_reasons(base: _HostResource, head: _HostResource) -> set[str]:
    changed_properties = {
        key
        for key in base.properties.keys() | head.properties.keys()
        if base.properties.get(key) != head.properties.get(key)
    }
    reasons: set[str] = set()
    for property_name in changed_properties:
        if property_name in _SERVICE_REDEPLOY_PROPERTIES:
            reasons.add(f"executor-host-service-{property_name}")
            continue
        if property_name in _SERVICE_NON_REDEPLOY_PROPERTIES:
            continue
        if property_name == "ForceNewDeployment":
            if _force_new_deployment_enabled(head.properties.get(property_name)):
                reasons.add("executor-host-service-ForceNewDeployment")
            continue
        raise TemplateClassificationError(f"Cannot classify changed ExecutorHost service property {property_name!r}")
    return reasons


def _force_new_deployment_enabled(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if not isinstance(value, Mapping):
        raise TemplateClassificationError("ForceNewDeployment must be a boolean or object")
    configuration = cast(Mapping[str, object], value)
    unsupported = set(configuration) - {"EnableForceNewDeployment", "ForceNewDeploymentNonce"}
    if unsupported:
        raise TemplateClassificationError(
            f"ForceNewDeployment contains unsupported fields: {', '.join(sorted(str(key) for key in unsupported))}"
        )
    enabled = configuration.get("EnableForceNewDeployment")
    if not isinstance(enabled, bool):
        raise TemplateClassificationError("ForceNewDeployment.EnableForceNewDeployment must be boolean")
    nonce = configuration.get("ForceNewDeploymentNonce")
    if nonce is not None and not isinstance(nonce, str):
        raise TemplateClassificationError("ForceNewDeployment.ForceNewDeploymentNonce must be a string")
    return enabled
