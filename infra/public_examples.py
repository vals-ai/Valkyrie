"""Public storage for reviewed benchmark examples and trajectories."""

from typing import cast

from aws_cdk import CfnOutput, RemovalPolicy, Stack
from aws_cdk import aws_cloudfront as cloudfront
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constants import PUBLIC_EXAMPLES_BUCKET_NAME
from constructs import Construct
from stage import Stage


class PublicExamples(Construct):
    """Private S3 origin and public read-only CDN for reviewed examples."""

    def __init__(self, scope: Construct, construct_id: str, *, stage: Stage) -> None:
        super().__init__(scope, construct_id)
        stack = Stack.of(self)
        bucket = s3.Bucket(
            self,
            "Bucket",
            bucket_name=f"{stage.phys(PUBLIC_EXAMPLES_BUCKET_NAME)}-{stack.account}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        origin_access_control = cloudfront.CfnOriginAccessControl(
            self,
            "OriginAccessControl",
            origin_access_control_config=cloudfront.CfnOriginAccessControl.OriginAccessControlConfigProperty(
                name=f"vals-public-benchmark-artifacts-{stage.name}",
                origin_access_control_origin_type="s3",
                signing_behavior="always",
                signing_protocol="sigv4",
            ),
        )
        cache_policy = cloudfront.CfnCachePolicy(
            self,
            "CachePolicy",
            cache_policy_config=cloudfront.CfnCachePolicy.CachePolicyConfigProperty(
                name=f"vals-public-benchmark-artifacts-{stage.name}",
                min_ttl=0,
                default_ttl=86_400,
                max_ttl=31_536_000,
                parameters_in_cache_key_and_forwarded_to_origin=(
                    cloudfront.CfnCachePolicy.ParametersInCacheKeyAndForwardedToOriginProperty(
                        cookies_config=cloudfront.CfnCachePolicy.CookiesConfigProperty(cookie_behavior="none"),
                        enable_accept_encoding_brotli=True,
                        enable_accept_encoding_gzip=True,
                        headers_config=cloudfront.CfnCachePolicy.HeadersConfigProperty(header_behavior="none"),
                        query_strings_config=cloudfront.CfnCachePolicy.QueryStringsConfigProperty(
                            query_string_behavior="none"
                        ),
                    )
                ),
            ),
        )
        response_headers_policy = cloudfront.CfnResponseHeadersPolicy(
            self,
            "ResponseHeadersPolicy",
            response_headers_policy_config=cloudfront.CfnResponseHeadersPolicy.ResponseHeadersPolicyConfigProperty(
                name=f"vals-public-benchmark-artifacts-{stage.name}",
                cors_config=cloudfront.CfnResponseHeadersPolicy.CorsConfigProperty(
                    access_control_allow_credentials=False,
                    access_control_allow_headers=(
                        cloudfront.CfnResponseHeadersPolicy.AccessControlAllowHeadersProperty(items=["*"])
                    ),
                    access_control_allow_methods=(
                        cloudfront.CfnResponseHeadersPolicy.AccessControlAllowMethodsProperty(items=["GET", "HEAD"])
                    ),
                    access_control_allow_origins=(
                        cloudfront.CfnResponseHeadersPolicy.AccessControlAllowOriginsProperty(items=["*"])
                    ),
                    origin_override=True,
                ),
                security_headers_config=cloudfront.CfnResponseHeadersPolicy.SecurityHeadersConfigProperty(
                    content_type_options=cloudfront.CfnResponseHeadersPolicy.ContentTypeOptionsProperty(override=True)
                ),
            ),
        )

        origin_id = "PublicExamplesS3Origin"
        distribution = cloudfront.CfnDistribution(
            self,
            "Distribution",
            distribution_config=cloudfront.CfnDistribution.DistributionConfigProperty(
                comment=f"Reviewed Vals benchmark examples ({stage.name})",
                default_cache_behavior=cloudfront.CfnDistribution.DefaultCacheBehaviorProperty(
                    allowed_methods=["GET", "HEAD"],
                    cached_methods=["GET", "HEAD"],
                    cache_policy_id=cache_policy.ref,
                    compress=True,
                    response_headers_policy_id=response_headers_policy.ref,
                    target_origin_id=origin_id,
                    viewer_protocol_policy="redirect-to-https",
                ),
                enabled=True,
                http_version="http2and3",
                ipv6_enabled=True,
                origins=[
                    cloudfront.CfnDistribution.OriginProperty(
                        domain_name=bucket.bucket_regional_domain_name,
                        id=origin_id,
                        origin_access_control_id=origin_access_control.attr_id,
                        s3_origin_config=cloudfront.CfnDistribution.S3OriginConfigProperty(origin_access_identity=""),
                    )
                ],
                price_class="PriceClass_100",
                viewer_certificate=cloudfront.CfnDistribution.ViewerCertificateProperty(
                    cloud_front_default_certificate=True
                ),
            ),
        )

        distribution_arn = stack.format_arn(
            service="cloudfront",
            region="",
            resource="distribution",
            resource_name=distribution.ref,
        )
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                conditions={"StringEquals": {"AWS:SourceArn": distribution_arn}},
                principals=[cast(iam.IPrincipal, iam.ServicePrincipal("cloudfront.amazonaws.com"))],
                resources=[
                    bucket.arn_for_objects("public-examples/*"),
                    bucket.arn_for_objects("public-example-pointers/*"),
                ],
            )
        )
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="RequireImmutablePublicExampleWrites",
                actions=["s3:PutObject"],
                conditions={"Null": {"s3:if-none-match": "true"}},
                effect=iam.Effect.DENY,
                principals=[cast(iam.IPrincipal, iam.AnyPrincipal())],
                resources=[bucket.arn_for_objects("public-examples/*")],
            )
        )
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="RequireConditionalPublicExamplePointerWrites",
                actions=["s3:PutObject"],
                conditions={"Null": {"s3:if-match": "true", "s3:if-none-match": "true"}},
                effect=iam.Effect.DENY,
                principals=[cast(iam.IPrincipal, iam.AnyPrincipal())],
                resources=[bucket.arn_for_objects("public-example-pointers/*")],
            )
        )

        self.bucket = bucket
        self.distribution = distribution
        CfnOutput(
            self,
            "BucketName",
            value=bucket.bucket_name,
            description="Private S3 bucket for reviewed public examples",
        )
        CfnOutput(
            self,
            "CloudFrontUrl",
            value=f"https://{distribution.attr_domain_name}",
            description="CloudFront base URL for reviewed public examples",
        )


__all__ = ["PublicExamples"]
