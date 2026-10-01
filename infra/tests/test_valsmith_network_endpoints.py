"""Exercise endpoint resource decisions, including role-scoped explicit denials."""

import copy
import fnmatch
import json
import unittest

from aws_cdk import assertions
from tests.test_valsmith_network_stack import network_stack, resources
from valsmith_network_config import JsonValue, object_field, object_list
from valsmith_network_endpoints import add_endpoints, endpoint_document, load_permissions

ROLE_PREFIX = "arn:aws:iam::629807611108:role/"
GENERATION = ROLE_PREFIX + "valsmith-generation-prod-task"
EVALUATION = ROLE_PREFIX + "valsmith-prod-task"
VIEW = ROLE_PREFIX + "ValSmithDatasetViewLambdaExecution"
POLICY = ROLE_PREFIX + "ValSmithProdBucketProvisi-BucketPolicyExecutionRole-UIgIgtbzpKl3"
EXECUTION = ROLE_PREFIX + "Prod-valsmithStack-valsmithTaskDefExecutionRole1AD4-BhTN1a8dQ2H1"
OWNER = "arn:aws:s3:::vs-prod-test/benchmarks/"


def patterns(value: JsonValue) -> list[str]:
    if isinstance(value, str):
        return [value]

    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return [item for item in value if isinstance(item, str)]

    raise AssertionError("Expected IAM strings")


def matches(value: str, expected: JsonValue, *, case_sensitive: bool = True) -> bool:
    return any(
        fnmatch.fnmatchcase(value if case_sensitive else value.lower(), pattern if case_sensitive else pattern.lower())
        for pattern in patterns(expected)
    )


def permitted(document: dict[str, JsonValue], action: str, resource: str, role: str, **context: str) -> bool:
    """Evaluate only the IAM operators used here; unsupported syntax fails the test."""
    values = {"aws:PrincipalArn": role, **context}
    allowed = False
    for statement in object_list(document, "Statement"):
        action_matches = matches(action, statement.get("Action", statement.get("NotAction")), case_sensitive=False)
        resource_matches = matches(resource, statement.get("Resource", statement.get("NotResource")))
        if action_matches == ("NotAction" in statement) or resource_matches == ("NotResource" in statement):
            continue

        conditions_match = True
        conditions = statement.get("Condition", {})
        if not isinstance(conditions, dict):
            raise AssertionError("Invalid condition")

        for operator in conditions:
            for key, expected in object_field(conditions, operator).items():
                actual = values.get(key)
                if operator in ("StringEquals", "ArnEquals"):
                    condition_matches = actual in patterns(expected)
                elif operator == "StringNotEquals":
                    condition_matches = actual not in patterns(expected)
                elif operator in ("StringLike", "ArnLike"):
                    condition_matches = actual is not None and matches(actual, expected)
                else:
                    raise AssertionError(f"Unsupported condition: {operator}")

                conditions_match = conditions_match and condition_matches

        if conditions_match and statement["Effect"] == "Deny":
            return False

        allowed = allowed or (conditions_match and statement["Effect"] == "Allow")

    return allowed


class EndpointPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.permissions = load_permissions()

    def test_owner_and_legacy_access_is_limited_by_role_path_and_account(self) -> None:
        document = endpoint_document("s3", self.permissions)
        cases = (
            (GENERATION, "s3:GetObject", OWNER + "valsmith-manifests/test/manifest.json", "629807611108", True),
            (GENERATION, "s3:GetObject", OWNER + "valsmith-manifests/test/private.txt", "629807611108", False),
            (GENERATION, "s3:GetObject", OWNER + "valsmith-manifests/test/manifest.json", "111111111111", False),
            (GENERATION, "s3:GetObject", "arn:aws:s3:::unrelated/private", "629807611108", False),
            (EVALUATION, "s3:GetObject", OWNER + "valsmith-datasets/test/manifest.json", "629807611108", True),
            (EVALUATION, "s3:PutObject", OWNER + "valsmith-datasets/test/manifest.json", "629807611108", False),
            (
                EVALUATION,
                "s3:GetObject",
                "arn:aws:s3:::agentic-harness/benchmarks/valsmith-datasets/test",
                "613431292675",
                True,
            ),
            (VIEW, "s3:GetObject", OWNER + "valsmith-dataset-views/test.json", "629807611108", True),
            (VIEW, "s3:PutObject", OWNER + "valsmith-dataset-views/test.json", "629807611108", True),
            (
                VIEW,
                "s3:PutObject",
                "arn:aws:s3:::agentic-harness/benchmarks/valsmith-dataset-views/test.json",
                "613431292675",
                False,
            ),
            (
                VIEW,
                "s3:GetObject",
                "arn:aws:s3:::vs-prod-test/nested/benchmarks/valsmith-dataset-views/test.json",
                "629807611108",
                False,
            ),
            (POLICY, "s3:PutBucketPolicy", "arn:aws:s3:::vs-prod-test", "629807611108", True),
            (POLICY, "s3:PutLifecycleConfiguration", "arn:aws:s3:::vs-prod-test", "629807611108", True),
            (POLICY, "s3:GetObject", OWNER + "valsmith-dataset-views/test.json", "629807611108", False),
            (POLICY, "s3:PutBucketPolicy", "arn:aws:s3:::vs-prod-test", "111111111111", False),
            (
                ROLE_PREFIX + "Unrelated",
                "s3:GetObject",
                OWNER + "valsmith-dataset-views/test.json",
                "629807611108",
                False,
            ),
        )
        for role, action, resource, account, expected in cases:
            with self.subTest(role=role, action=action, resource=resource, account=account):
                self.assertEqual(
                    permitted(document, action, resource, role, **{"s3:ResourceAccount": account}), expected
                )

    def test_list_prefix_and_ecr_layer_exception_do_not_grant_customer_access(self) -> None:
        document = endpoint_document("s3", self.permissions)
        for prefix, expected in (("benchmarks/valsmith-datasets/test", True), ("agents/", False), ("", False)):
            self.assertEqual(
                permitted(
                    document, "s3:ListBucket", "arn:aws:s3:::agentic-harness", EVALUATION, **{"s3:prefix": prefix}
                ),
                expected,
            )

        layer = "arn:aws:s3:::prod-us-east-1-starport-layer-bucket/layer"
        self.assertTrue(permitted(document, "s3:GetObject", layer, "ecr-service-identity"))
        self.assertFalse(permitted(document, "s3:PutObject", layer, "ecr-service-identity"))
        self.assertFalse(permitted(document, "s3:GetObject", OWNER + "valsmith-datasets/test", "ecr-service-identity"))

    def test_deny_statements_remain_bound_to_their_original_role(self) -> None:
        document = endpoint_document("s3", self.permissions)
        document["Statement"] = [
            *object_list(document, "Statement"),
            {"Effect": "Allow", "Principal": "*", "Action": "s3:*", "Resource": "*"},
        ]
        self.assertFalse(permitted(document, "s3:GetObject", "arn:aws:s3:::unrelated/private", GENERATION))
        self.assertFalse(
            permitted(
                document,
                "s3:DeleteBucket",
                "arn:aws:s3:::vs-prod-test",
                POLICY,
                **{"s3:ResourceAccount": "629807611108"},
            )
        )
        self.assertTrue(
            permitted(
                document,
                "s3:GetObject",
                OWNER + "valsmith-dataset-views/test",
                VIEW,
                **{"s3:ResourceAccount": "629807611108"},
            )
        )

    def test_startup_permissions_and_quota_writes_do_not_cross_resources(self) -> None:
        tests = (
            (
                "dynamodb",
                EVALUATION,
                "dynamodb:UpdateItem",
                "arn:aws:dynamodb:us-east-1:629807611108:table/Prod-valsmithStack-valsmithEvaluationQuotaTableA4DC6856-PQS1865PH4M6",
            ),
            (
                "secretsmanager",
                EXECUTION,
                "secretsmanager:GetSecretValue",
                "arn:aws:secretsmanager:us-east-1:629807611108:secret:benchmark-services/valsmith-api-access-key-ABC123",
            ),
            (
                "secretsmanager",
                VIEW,
                "secretsmanager:GetSecretValue",
                "arn:aws:secretsmanager:us-east-1:629807611108:secret:prodModelGatewayClientConfig-ABC123",
            ),
            (
                "ssm",
                EXECUTION,
                "ssm:GetParameters",
                "arn:aws:ssm:us-east-1:629807611108:parameter/benchmark-services/descope/project-id",
            ),
            (
                "logs",
                EXECUTION,
                "logs:PutLogEvents",
                "arn:aws:logs:us-east-1:629807611108:log-group:Prod-valsmithStack-valsmithLogGroup891402E9-UEXTRQL37tTw:log-stream:test",
            ),
            (
                "ecr",
                EXECUTION,
                "ecr:BatchGetImage",
                "arn:aws:ecr:us-east-1:629807611108:repository/cdk-hnb659fds-container-assets-629807611108-us-east-1",
            ),
        )
        for service, role, action, resource in tests:
            document = endpoint_document(service, self.permissions)
            with self.subTest(service=service, role=role):
                self.assertTrue(permitted(document, action, resource, role))
                self.assertFalse(permitted(document, action, resource.replace("629807611108", "111111111111"), role))
                self.assertFalse(permitted(document, action, resource, ROLE_PREFIX + "Unrelated"))
                self.assertFalse(
                    permitted(
                        document,
                        action,
                        resource + "/unrelated"
                        if service in ("ssm", "ecr", "dynamodb")
                        else resource.replace(":test", ":wrong")
                        .replace("LogGroup891402E9", "WrongGroup")
                        .replace("ClientConfig", "Other")
                        .replace("access-key", "other"),
                        role,
                    )
                )

        self.assertFalse(
            permitted(endpoint_document("ssm", self.permissions), "ssm:GetParameterHistory", tests[3][3], EXECUTION)
        )

    def test_conflicting_principal_conditions_are_rejected(self) -> None:
        permissions = copy.deepcopy(self.permissions)
        permissions["s3"][GENERATION][0]["Condition"] = {"ArnEquals": {"aws:PrincipalArn": ROLE_PREFIX + "Other"}}
        with self.assertRaisesRegex(ValueError, "PrincipalArn"):
            endpoint_document("s3", permissions)

    def test_documents_fit_endpoint_size_and_bind_every_non_layer_statement(self) -> None:
        for service in self.permissions:
            document = endpoint_document(service, self.permissions)
            self.assertLess(len(json.dumps(document).encode()), 20_480)
            for statement in object_list(document, "Statement"):
                self.assertEqual(statement["Principal"], "*")
                if statement.get("Resource") == "arn:aws:s3:::prod-us-east-1-starport-layer-bucket/*":
                    self.assertEqual(statement["Action"], "s3:GetObject")
                    continue

                principal = object_field(object_field(statement, "Condition"), "ArnEquals")["aws:PrincipalArn"]
                self.assertIn(principal, self.permissions[service])

    def test_endpoint_routes_and_https_sources_are_limited_to_new_network(self) -> None:
        stack = network_stack()
        add_endpoints(stack, self.permissions)
        template = assertions.Template.from_stack(stack)
        endpoints = resources(template, "AWS::EC2::VPCEndpoint")
        self.assertEqual(len(endpoints), 7)
        for endpoint in endpoints.values():
            self.assertEqual(endpoint["VpcId"], stack.resolve(stack.vpc.vpc_id))
            self.assertIn("PolicyDocument", endpoint)
            if endpoint["VpcEndpointType"] == "Interface":
                self.assertTrue(endpoint["PrivateDnsEnabled"])
                self.assertEqual(
                    endpoint["SubnetIds"], [stack.resolve(subnet.subnet_id) for subnet in stack.application_subnets]
                )

        policy_id = stack.resolve(stack.policy_group.security_group_id)
        for rule in resources(template, "AWS::EC2::SecurityGroupEgress").values():
            if rule["GroupId"] == policy_id:
                if rule["IpProtocol"] == "icmp":
                    self.assertEqual(
                        (rule["CidrIp"], rule["FromPort"], rule["ToPort"]), ("255.255.255.255/32", 252, 86)
                    )
                    continue

                self.assertEqual(rule.get("DestinationPrefixListId"), "pl-63a5400a")
                self.assertEqual(rule["FromPort"], 443)
            if rule.get("FromPort") == 443 and rule["GroupId"] != stack.resolve(stack.proxy_group.security_group_id):
                self.assertNotIn("CidrIp", rule)
