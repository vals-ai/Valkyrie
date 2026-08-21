import copy
import unittest
from collections.abc import Mapping
from typing import cast

from classify_executor_template_change import (
    TemplateClassificationError,
    classify_executor_host_template_change,
)

_STACK_ID = "WorkerStack"
_TASK_ID = "ExecutorHostTaskDefAC021289"
_SERVICE_ID = "ExecutorHostService2E3578AB"


def _template(
    *,
    stack_id: str = _STACK_ID,
    include_task: bool = True,
    include_service: bool = True,
) -> dict[str, object]:
    resources: dict[str, object] = {
        "UnrelatedLogGroup": {
            "Type": "AWS::Logs::LogGroup",
            "Properties": {"RetentionInDays": 30},
            "Metadata": {"aws:cdk:path": f"{stack_id}/UnrelatedLogGroup/Resource"},
        }
    }
    if include_task:
        resources[_TASK_ID] = {
            "Type": "AWS::ECS::TaskDefinition",
            "Properties": {
                "ContainerDefinitions": [
                    {
                        "Name": "ExecutorHost",
                        "Image": "image:base",
                        "Environment": [{"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "false"}],
                        "Secrets": [{"Name": "DATABASE_URL", "ValueFrom": "arn:secret:base"}],
                    }
                ],
                "Cpu": "4096",
                "Memory": "8192",
                "NetworkMode": "awsvpc",
                "Tags": [{"Key": "stage", "Value": "prod"}],
            },
            "Metadata": {"aws:cdk:path": f"{stack_id}/ExecutorHostTaskDef/Resource"},
        }
    if include_service:
        resources[_SERVICE_ID] = {
            "Type": "AWS::ECS::Service",
            "Properties": {
                "Cluster": {"Fn::ImportValue": "SharedCluster"},
                "DeploymentConfiguration": {"MaximumPercent": 200, "MinimumHealthyPercent": 100},
                "DeploymentController": {"Type": "ECS"},
                "DesiredCount": 1,
                "EnableECSManagedTags": False,
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {"AwsvpcConfiguration": {"AssignPublicIp": "ENABLED"}},
                "ServiceName": "ExecutorHost",
                "TaskDefinition": {"Ref": _TASK_ID},
            },
            "Metadata": {"aws:cdk:path": f"{stack_id}/ExecutorHostService/Service"},
        }
    return {"Resources": resources}


def _resource(template: Mapping[str, object], logical_id: str) -> dict[str, object]:
    raw_resources = template["Resources"]
    assert isinstance(raw_resources, dict)
    resources = cast(dict[str, object], raw_resources)
    raw_resource = resources[logical_id]
    assert isinstance(raw_resource, dict)
    return cast(dict[str, object], raw_resource)


def _properties(template: Mapping[str, object], logical_id: str) -> dict[str, object]:
    resource = _resource(template, logical_id)
    raw_properties = resource["Properties"]
    assert isinstance(raw_properties, dict)
    return cast(dict[str, object], raw_properties)


class ExecutorTemplateClassifierTest(unittest.TestCase):
    def _classify(
        self,
        base: Mapping[str, object],
        head: Mapping[str, object],
        *,
        stack_id: str = _STACK_ID,
    ):
        return classify_executor_host_template_change(base, head, expected_stack_id=stack_id)

    def test_identical_host_resources_do_not_require_redeploy(self) -> None:
        base = _template()
        head = copy.deepcopy(base)
        _properties(head, "UnrelatedLogGroup")["RetentionInDays"] = 90
        _resource(head, _TASK_ID)["Metadata"] = {
            "aws:cdk:path": f"{_STACK_ID}/ExecutorHostTaskDef/Resource",
            "changed": True,
        }

        effect = self._classify(base, head)

        self.assertFalse(effect.redeploy_required)
        self.assertEqual(effect.reasons, ())

    def test_task_definition_cpu_and_memory_changes_do_not_require_maintenance(self) -> None:
        for changes in ({"Cpu": "8192"}, {"Memory": "32768"}, {"Cpu": "8192", "Memory": "32768"}):
            with self.subTest(changes=changes):
                base = _template()
                head = copy.deepcopy(base)
                _properties(head, _TASK_ID).update(changes)

                effect = self._classify(base, head)

                self.assertFalse(effect.redeploy_required)
                self.assertEqual(effect.reasons, ())

    def test_task_definition_other_property_change_requires_redeploy(self) -> None:
        base = _template()
        head = copy.deepcopy(base)
        task_properties = _properties(head, _TASK_ID)
        task_properties["Cpu"] = "8192"
        task_properties["ContainerDefinitions"] = [{"Name": "ExecutorHost", "Image": "image:changed"}]

        effect = self._classify(base, head)

        self.assertTrue(effect.redeploy_required)
        self.assertEqual(effect.reasons, ("executor-host-task-definition-changed",))

    def test_container_environment_only_change_does_not_require_maintenance(self) -> None:
        base = _template()
        head = copy.deepcopy(base)
        containers = _properties(head, _TASK_ID)["ContainerDefinitions"]
        assert isinstance(containers, list)
        container = cast(dict[str, object], containers[0])
        container["Environment"] = [
            {"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "true"},
            {"Name": "AWS_DEPLOYMENT_ROLE_ORG_IDS", "Value": "org"},
        ]

        effect = self._classify(base, head)

        self.assertFalse(effect.redeploy_required)
        self.assertEqual(effect.reasons, ())

    def test_container_change_beyond_environment_requires_maintenance(self) -> None:
        for key, value in (
            ("Image", "image:changed"),
            ("Secrets", [{"Name": "DATABASE_URL", "ValueFrom": "arn:secret:changed"}]),
            ("Name", "RenamedExecutorHost"),
        ):
            with self.subTest(key=key):
                base = _template()
                head = copy.deepcopy(base)
                containers = _properties(head, _TASK_ID)["ContainerDefinitions"]
                assert isinstance(containers, list)
                container = cast(dict[str, object], containers[0])
                container["Environment"] = [{"Name": "AWS_MANAGED_SUBMISSIONS_ENABLED", "Value": "true"}]
                container[key] = value

                effect = self._classify(base, head)

                self.assertTrue(effect.redeploy_required)
                self.assertEqual(effect.reasons, ("executor-host-task-definition-changed",))

    def test_added_container_requires_maintenance(self) -> None:
        base = _template()
        head = copy.deepcopy(base)
        containers = _properties(head, _TASK_ID)["ContainerDefinitions"]
        assert isinstance(containers, list)
        cast(list[object], containers).append({"Name": "Sidecar", "Image": "image:sidecar"})

        effect = self._classify(base, head)

        self.assertTrue(effect.redeploy_required)
        self.assertEqual(effect.reasons, ("executor-host-task-definition-changed",))

    def test_malformed_container_definitions_are_rejected(self) -> None:
        for containers in ("ExecutorHost", ["ExecutorHost"]):
            with self.subTest(containers=containers):
                base = _template()
                head = copy.deepcopy(base)
                _properties(head, _TASK_ID)["ContainerDefinitions"] = containers

                with self.assertRaisesRegex(TemplateClassificationError, "ContainerDefinitions"):
                    self._classify(base, head)

    def test_task_definition_tags_do_not_require_redeploy(self) -> None:
        base = _template()
        head = copy.deepcopy(base)
        _properties(head, _TASK_ID)["Tags"] = [{"Key": "stage", "Value": "changed"}]

        self.assertFalse(self._classify(base, head).redeploy_required)

    def test_task_definition_logical_id_or_type_change_requires_redeploy(self) -> None:
        for change in ("logical-id", "type"):
            with self.subTest(change=change):
                base = _template()
                head = copy.deepcopy(base)
                if change == "logical-id":
                    raw_resources = head["Resources"]
                    assert isinstance(raw_resources, dict)
                    resources = cast(dict[str, object], raw_resources)
                    resources["ChangedTaskDefinition"] = resources.pop(_TASK_ID)
                    _properties(head, _SERVICE_ID)["TaskDefinition"] = {"Ref": "ChangedTaskDefinition"}
                else:
                    _resource(head, _TASK_ID)["Type"] = "AWS::ECS::Service"

                effect = self._classify(base, head)

                self.assertTrue(effect.redeploy_required)
                self.assertIn("executor-host-task-definition-replaced", effect.reasons)

    def test_documented_service_rollout_properties_require_redeploy(self) -> None:
        changed_values: dict[str, object] = {
            "Cluster": "changed-cluster",
            "LaunchType": "EC2",
            "LoadBalancers": [{"TargetGroupArn": "changed"}],
            "NetworkConfiguration": {"AwsvpcConfiguration": {"AssignPublicIp": "DISABLED"}},
            "PlatformVersion": "1.4.0",
            "Role": "changed-role",
            "SchedulingStrategy": "DAEMON",
            "ServiceConnectConfiguration": {"Enabled": True},
            "ServiceName": "ChangedExecutorHost",
            "ServiceRegistries": [{"RegistryArn": "changed"}],
            "TaskDefinition": "changed-task-definition",
            "VolumeConfigurations": [{"Name": "data"}],
            "VpcLatticeConfigurations": [{"RoleArn": "changed"}],
        }
        for property_name, changed_value in changed_values.items():
            with self.subTest(property_name=property_name):
                base = _template()
                head = copy.deepcopy(base)
                _properties(head, _SERVICE_ID)[property_name] = changed_value

                effect = self._classify(base, head)

                self.assertTrue(effect.redeploy_required)
                self.assertEqual(effect.reasons, (f"executor-host-service-{property_name}",))

    def test_documented_service_non_rollout_properties_do_not_require_redeploy(self) -> None:
        changed_values: dict[str, object] = {
            "AvailabilityZoneRebalancing": "ENABLED",
            "CapacityProviderStrategy": [{"CapacityProvider": "FARGATE_SPOT"}],
            "DeploymentConfiguration": {"MaximumPercent": 150, "MinimumHealthyPercent": 100},
            "DesiredCount": 4,
            "EnableECSManagedTags": True,
            "EnableExecuteCommand": True,
            "HealthCheckGracePeriodSeconds": 30,
            "PlacementConstraints": [{"Type": "distinctInstance"}],
            "PlacementStrategies": [{"Type": "spread", "Field": "attribute:ecs.availability-zone"}],
            "PropagateTags": "SERVICE",
            "Tags": [{"Key": "stage", "Value": "changed"}],
        }
        for property_name, changed_value in changed_values.items():
            with self.subTest(property_name=property_name):
                base = _template()
                head = copy.deepcopy(base)
                _properties(head, _SERVICE_ID)[property_name] = changed_value

                self.assertFalse(self._classify(base, head).redeploy_required)

    def test_force_new_deployment_is_asymmetric(self) -> None:
        cases = (
            (None, {"EnableForceNewDeployment": True}, True),
            (
                {"EnableForceNewDeployment": True, "ForceNewDeploymentNonce": "one"},
                {"EnableForceNewDeployment": True, "ForceNewDeploymentNonce": "two"},
                True,
            ),
            ({"EnableForceNewDeployment": True}, {"EnableForceNewDeployment": False}, False),
            (True, False, False),
        )
        for base_value, head_value, expected in cases:
            with self.subTest(base=base_value, head=head_value):
                base = _template()
                head = copy.deepcopy(base)
                if base_value is not None:
                    _properties(base, _SERVICE_ID)["ForceNewDeployment"] = base_value
                _properties(head, _SERVICE_ID)["ForceNewDeployment"] = head_value

                self.assertEqual(self._classify(base, head).redeploy_required, expected)

    def test_unsupported_service_property_change_is_a_technical_error(self) -> None:
        base = _template()
        head = copy.deepcopy(base)
        _properties(head, _SERVICE_ID)["DeploymentController"] = {"Type": "CODE_DEPLOY"}

        with self.assertRaisesRegex(TemplateClassificationError, "DeploymentController"):
            self._classify(base, head)

    def test_host_lifecycle_transitions_are_explicit(self) -> None:
        absent = _template(include_task=False, include_service=False)
        present = _template()

        self.assertFalse(self._classify(absent, absent).redeploy_required)
        self.assertFalse(self._classify(absent, present).redeploy_required)
        removed = self._classify(present, absent)
        self.assertTrue(removed.redeploy_required)
        self.assertEqual(removed.reasons, ("executor-host-removed",))

    def test_partial_or_duplicate_host_resources_are_technical_errors(self) -> None:
        partial = _template(include_service=False)
        with self.assertRaisesRegex(TemplateClassificationError, "both ExecutorHost"):
            self._classify(partial, _template())

        duplicate = _template()
        raw_resources = duplicate["Resources"]
        assert isinstance(raw_resources, dict)
        resources = cast(dict[str, object], raw_resources)
        resources["DuplicateTask"] = copy.deepcopy(resources[_TASK_ID])
        with self.assertRaisesRegex(TemplateClassificationError, "duplicate"):
            self._classify(_template(), duplicate)

    def test_wrong_stack_or_malformed_template_is_a_technical_error(self) -> None:
        with self.assertRaisesRegex(TemplateClassificationError, "another stack"):
            self._classify(_template(stack_id="ValkDevWorkerStack"), _template(stack_id="ValkDevWorkerStack"))
        with self.assertRaisesRegex(TemplateClassificationError, "Resources"):
            self._classify({}, _template())


if __name__ == "__main__":
    unittest.main()
