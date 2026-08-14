"""Tests for the reviewed public-example storage boundary."""

import json
import unittest

import aws_cdk as cdk
from aws_cdk import assertions

from shared import SharedStack
from stage import DEV, RELEASE_TEST, Stage

TEST_ACCOUNT = "123456789012"
TEST_REGION = "us-east-1"
TEST_ENV = cdk.Environment(account=TEST_ACCOUNT, region=TEST_REGION)


def dev_template() -> assertions.Template:
    app = cdk.App()
    stack = SharedStack(app, Stage(DEV).stack_id("SharedStack"), stage=Stage(DEV), env=TEST_ENV)
    return assertions.Template.from_stack(stack)


class PublicExamplesInfrastructureTest(unittest.TestCase):
    def test_dev_public_example_bucket_is_dedicated_private_and_retained(self) -> None:
        buckets = dev_template().find_resources("AWS::S3::Bucket")
        public_buckets = [
            bucket
            for bucket in buckets.values()
            if bucket["Properties"].get("BucketName") == f"vals-public-benchmark-artifacts-dev-{TEST_ACCOUNT}"
        ]
        self.assertEqual(len(public_buckets), 1)
        public_bucket = public_buckets[0]

        self.assertEqual(public_bucket["DeletionPolicy"], "Retain")
        self.assertEqual(public_bucket["UpdateReplacePolicy"], "Retain")
        self.assertEqual(public_bucket["Properties"]["VersioningConfiguration"], {"Status": "Enabled"})
        self.assertEqual(
            public_bucket["Properties"]["PublicAccessBlockConfiguration"],
            {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        )
        self.assertEqual(
            public_bucket["Properties"]["BucketEncryption"],
            {"ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
        )
        self.assertEqual(
            public_bucket["Properties"]["OwnershipControls"],
            {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )

    def test_dev_cloudfront_is_read_only_and_bucket_policy_is_prefix_scoped(self) -> None:
        template = dev_template()
        controls = template.find_resources("AWS::CloudFront::OriginAccessControl")
        self.assertEqual(len(controls), 1)
        control = next(iter(controls.values()))["Properties"]["OriginAccessControlConfig"]
        self.assertEqual(control["OriginAccessControlOriginType"], "s3")
        self.assertEqual(control["SigningBehavior"], "always")
        self.assertEqual(control["SigningProtocol"], "sigv4")

        distributions = template.find_resources("AWS::CloudFront::Distribution")
        self.assertEqual(len(distributions), 1)
        distribution_id, distribution = next(iter(distributions.items()))
        behavior = distribution["Properties"]["DistributionConfig"]["DefaultCacheBehavior"]
        self.assertEqual(behavior["AllowedMethods"], ["GET", "HEAD"])
        self.assertEqual(behavior["CachedMethods"], ["GET", "HEAD"])
        self.assertEqual(behavior["ViewerProtocolPolicy"], "redirect-to-https")

        cache_policy = next(iter(template.find_resources("AWS::CloudFront::CachePolicy").values()))["Properties"][
            "CachePolicyConfig"
        ]
        self.assertEqual(
            (cache_policy["MinTTL"], cache_policy["DefaultTTL"], cache_policy["MaxTTL"]),
            (0, 86_400, 31_536_000),
        )
        headers_policy = next(iter(template.find_resources("AWS::CloudFront::ResponseHeadersPolicy").values()))[
            "Properties"
        ]["ResponseHeadersPolicyConfig"]
        self.assertEqual(
            headers_policy["CorsConfig"],
            {
                "AccessControlAllowCredentials": False,
                "AccessControlAllowHeaders": {"Items": ["*"]},
                "AccessControlAllowMethods": {"Items": ["GET", "HEAD"]},
                "AccessControlAllowOrigins": {"Items": ["*"]},
                "OriginOverride": True,
            },
        )

        policies = template.find_resources("AWS::S3::BucketPolicy")
        public_policy = next(
            policy for policy in policies.values() if "RequireImmutablePublicExampleWrites" in json.dumps(policy)
        )
        statements = public_policy["Properties"]["PolicyDocument"]["Statement"]
        cloudfront_read = next(statement for statement in statements if statement["Action"] == "s3:GetObject")
        self.assertEqual(cloudfront_read["Principal"], {"Service": "cloudfront.amazonaws.com"})
        self.assertEqual(
            cloudfront_read["Condition"]["StringEquals"]["AWS:SourceArn"],
            {
                "Fn::Join": [
                    "",
                    [
                        "arn:",
                        {"Ref": "AWS::Partition"},
                        f":cloudfront::{TEST_ACCOUNT}:distribution/",
                        {"Ref": distribution_id},
                    ],
                ]
            },
        )
        self.assertEqual(
            {resource["Fn::Join"][1][-1] for resource in cloudfront_read["Resource"]},
            {"/public-example-pointers/*", "/public-examples/*"},
        )
        immutable_write = next(
            statement for statement in statements if statement.get("Sid") == "RequireImmutablePublicExampleWrites"
        )
        self.assertEqual(immutable_write["Effect"], "Deny")
        self.assertEqual(immutable_write["Principal"], {"AWS": "*"})
        self.assertEqual(immutable_write["Condition"], {"Null": {"s3:if-none-match": "true"}})
        pointer_write = next(
            statement
            for statement in statements
            if statement.get("Sid") == "RequireConditionalPublicExamplePointerWrites"
        )
        self.assertEqual(
            pointer_write["Condition"],
            {"Null": {"s3:if-match": "true", "s3:if-none-match": "true"}},
        )
        self.assertEqual(pointer_write["Effect"], "Deny")
        self.assertEqual(pointer_write["Principal"], {"AWS": "*"})

    def test_dev_stack_outputs_publication_destination(self) -> None:
        outputs = dev_template().to_json().get("Outputs", {})
        self.assertEqual(
            {output["Description"] for output in outputs.values()},
            {
                "CloudFront base URL for reviewed public examples",
                "Private S3 bucket for reviewed public examples",
            },
        )

    def test_release_test_does_not_create_public_hosting(self) -> None:
        app = cdk.App()
        stage = Stage(RELEASE_TEST)
        stack = SharedStack(app, stage.stack_id("SharedStack"), stage=stage, env=TEST_ENV)
        template = assertions.Template.from_stack(stack)

        self.assertEqual(len(template.find_resources("AWS::S3::Bucket")), 1)
        self.assertEqual(template.find_resources("AWS::CloudFront::Distribution"), {})


if __name__ == "__main__":
    unittest.main()
