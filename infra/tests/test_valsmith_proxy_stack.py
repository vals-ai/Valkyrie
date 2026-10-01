"""Qualify the standalone deployment and the two-zone proxy task boundary."""

import json
import tempfile
import unittest
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aws_cdk import App, Environment, assertions
from tests.test_valsmith_network_config import read_test_inputs, resource_fixture
from tests.test_valsmith_network_preflight import FixtureReader
from tests.test_valsmith_network_stack import PROXY_IMAGE, resources
from valsmith_network_app import build_stack, verify_deployment_inputs
from valsmith_network_config import NetworkInventory, input_hash, json_document, object_field, object_list
from valsmith_proxy_stack import ValSmithProxyImagesStack


class ProxyStackTest(unittest.TestCase):
    def test_saved_preflight_cannot_replace_fresh_aws_identity_or_expire(self) -> None:
        inputs = read_test_inputs()
        current = NetworkInventory(
            inputs.account_id, inputs.region, datetime.now(UTC), input_hash(inputs), (), resource_fixture()
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "preflight.json"
            path.write_text(json.dumps(asdict(current), default=str))
            reader = FixtureReader({("sts", "get-caller-identity"): {"Account": "629807611108"}})
            verify_deployment_inputs(path, inputs, reader)
            wrong = FixtureReader({("sts", "get-caller-identity"): {"Account": "613431292675"}})
            with self.assertRaises(ValueError):
                verify_deployment_inputs(path, inputs, wrong)
            expired = asdict(current) | {"observed_at": (datetime.now(UTC) - timedelta(minutes=16)).isoformat()}
            path.write_text(json.dumps(expired, default=str))
            with self.assertRaisesRegex(ValueError, "15 minutes"):
                verify_deployment_inputs(path, inputs, reader)

    def test_image_stack_keeps_immutable_rollback_images(self) -> None:
        app = App()
        stack = ValSmithProxyImagesStack(
            app, "ValSmithProdProxyImages", env=Environment(account="629807611108", region="us-east-1")
        )
        template = assertions.Template.from_stack(stack)
        template.resource_count_is("AWS::ECR::Repository", 1)
        repository = next(iter(template.find_resources("AWS::ECR::Repository").values()))
        self.assertEqual(repository["DeletionPolicy"], "Retain")
        properties = repository["Properties"]
        self.assertEqual(properties["RepositoryName"], "valsmith-outbound-proxy-prod")
        self.assertEqual(properties["ImageTagMutability"], "IMMUTABLE")
        self.assertTrue(properties["ImageScanningConfiguration"]["ScanOnPush"])
        self.assertNotIn("LifecyclePolicy", properties)
        self.assertNotIn("EmptyOnDelete", properties)
        template.resource_count_is("AWS::EC2::VPC", 0)

    def test_app_instantiates_exactly_the_selected_component(self) -> None:
        for component, name in (("images", "ValSmithProdProxyImages"), ("network", "ValSmithProdNetwork")):
            app = App()
            build_stack(app, component, read_test_inputs(), PROXY_IMAGE)
            assembly = app.synth()
            self.assertEqual([stack.stack_name for stack in assembly.stacks], [name])
            self.assertFalse(json_document((Path(assembly.directory) / "manifest.json").read_text()).get("missing"))

        for component in ("", "all", "tracker"):
            with self.assertRaises(ValueError):
                build_stack(App(), component, read_test_inputs(), PROXY_IMAGE)
        with self.assertRaises(ValueError):
            build_stack(App(), "network", read_test_inputs(), "latest")

    def test_two_one_zone_services_use_unprivileged_tasks_and_empty_task_role(self) -> None:
        stack = build_stack(App(), "network", read_test_inputs(), PROXY_IMAGE)
        template = assertions.Template.from_stack(stack)
        template.resource_count_is("AWS::ECS::Cluster", 1)
        template.resource_count_is("AWS::ECS::Service", 2)
        template.resource_count_is("AWS::ECS::TaskDefinition", 1)
        services = resources(template, "AWS::ECS::Service")
        subnet_sets: list[object] = []
        for service in services.values():
            self.assertEqual(service["DesiredCount"], 1)
            self.assertFalse(service["EnableExecuteCommand"])
            network = object_field(object_field(service, "NetworkConfiguration"), "AwsvpcConfiguration")
            self.assertEqual(network["AssignPublicIp"], "ENABLED")
            subnets = network["Subnets"]
            assert isinstance(subnets, list)
            self.assertEqual(len(subnets), 1)
            subnet_sets.append(subnets)
        self.assertNotEqual(subnet_sets[0], subnet_sets[1])

        task = next(iter(resources(template, "AWS::ECS::TaskDefinition").values()))
        self.assertEqual(task["Cpu"], "512")
        self.assertEqual(task["Memory"], "1024")
        self.assertEqual(task["RuntimePlatform"], {"CpuArchitecture": "ARM64", "OperatingSystemFamily": "LINUX"})
        container = object_list(task, "ContainerDefinitions")[0]
        self.assertEqual(
            container["Image"],
            {
                "Fn::Join": [
                    "",
                    [
                        "629807611108.dkr.ecr.us-east-1.",
                        {"Ref": "AWS::URLSuffix"},
                        "/valsmith-outbound-proxy-prod@sha256:" + "a" * 64,
                    ],
                ]
            },
        )
        self.assertEqual(container["User"], "13")
        self.assertTrue(container["ReadonlyRootFilesystem"])
        self.assertEqual(object_field(object_field(container, "LinuxParameters"), "Capabilities")["Drop"], ["ALL"])
        self.assertEqual(
            container["MountPoints"], [{"ContainerPath": "/tmp", "ReadOnly": False, "SourceVolume": "proxy-tmp"}]
        )
        self.assertNotIn("Secrets", container)
        roles = resources(template, "AWS::IAM::Role")
        task_role = next(properties for name, properties in roles.items() if "ProxyTaskRole" in name)
        self.assertNotIn("Policies", task_role)
        self.assertNotIn("ManagedPolicyArns", task_role)
        task_role_id = next(name for name in roles if "ProxyTaskRole" in name)
        for policy in resources(template, "AWS::IAM::Policy").values():
            assigned_roles = policy["Roles"]
            assert isinstance(assigned_roles, list)
            self.assertNotIn({"Ref": task_role_id}, assigned_roles)
            statements = object_list(object_field(policy, "PolicyDocument"), "Statement")
            actions: set[str] = set()
            for statement in statements:
                action = statement["Action"]
                if isinstance(action, str):
                    actions.add(action)
                else:
                    assert isinstance(action, list) and all(isinstance(item, str) for item in action)
                    actions.update(item for item in action if isinstance(item, str))
                if statement["Resource"] == "*":
                    self.assertEqual(action, "ecr:GetAuthorizationToken")
                else:
                    resource = json.dumps(statement["Resource"])
                    self.assertTrue("repository/valsmith-outbound-proxy-prod" in resource or "ProxyLogs" in resource)
            self.assertEqual(
                actions,
                {
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetAuthorizationToken",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                },
            )

        rendered_roles = json.dumps(roles)
        for denied in ("secretsmanager:", "s3:", "dynamodb:", "ssm:", "AdministratorAccess"):
            self.assertNotIn(denied, rendered_roles)

    def test_both_internal_listeners_share_redundant_targets(self) -> None:
        stack = build_stack(App(), "network", read_test_inputs(), PROXY_IMAGE)
        template = assertions.Template.from_stack(stack)
        listeners = resources(template, "AWS::ElasticLoadBalancingV2::Listener").values()
        self.assertEqual(sorted(str(listener["Port"]) for listener in listeners), ["3128", "3129"])
        groups = resources(template, "AWS::ElasticLoadBalancingV2::TargetGroup")
        self.assertEqual(len(groups), 2)
        for group in groups.values():
            self.assertEqual(group["TargetType"], "ip")
            self.assertEqual(group["Protocol"], "TCP")
            attributes = object_list(group, "TargetGroupAttributes")
            self.assertIn({"Key": "preserve_client_ip.enabled", "Value": "false"}, attributes)
        for service in resources(template, "AWS::ECS::Service").values():
            self.assertEqual(len(object_list(service, "LoadBalancers")), 2)
