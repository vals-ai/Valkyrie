"""GitHub Actions access for deploying Valkyrie to the development account."""

from typing import Any, cast

import aws_cdk as cdk
from aws_cdk import Stack, aws_iam, aws_ssm
from constructs import Construct

GITHUB_OIDC_ISSUER = "token.actions.githubusercontent.com"
GITHUB_OIDC_URL = f"https://{GITHUB_OIDC_ISSUER}"
GITHUB_REPOSITORY_ID = "1084629789"
GITHUB_REPOSITORY_OWNER_ID = "129814943"
GITHUB_DEV_REF = "refs/heads/dev"
GITHUB_DEV_ENVIRONMENT = "dev"
GITHUB_DEV_SUBJECT = "repo:vals-ai/Valkyrie:environment:dev"
GITHUB_OIDC_AUDIENCE = "sts.amazonaws.com"

CDK_BOOTSTRAP_QUALIFIER = "hnb659fds"
CDK_BOOTSTRAP_ROLE_TYPES = ("lookup", "deploy", "file-publishing", "image-publishing")

OIDC_PROVIDER_ARN_PARAMETER = "/vals/dev/github/oidc-provider-arn"
VALKYRIE_ROLE_ARN_PARAMETER = "/vals/dev/github/valkyrie-role-arn"


class DeploymentAccessStack(Stack):
    """Create the narrowly scoped GitHub identity used for dev deployments."""

    def __init__(self, scope: Construct, id: str, **kwargs: Any) -> None:
        super().__init__(scope, id, **kwargs)

        if cdk.Token.is_unresolved(self.account) or cdk.Token.is_unresolved(self.region):
            raise ValueError("DeploymentAccessStack requires a concrete AWS account and region")

        self.oidc_provider = aws_iam.CfnOIDCProvider(
            self,
            "GitHubOidcProvider",
            url=GITHUB_OIDC_URL,
            client_id_list=[GITHUB_OIDC_AUDIENCE],
        )

        cdk_role_arns = [
            f"arn:aws:iam::{self.account}:role/"
            f"cdk-{CDK_BOOTSTRAP_QUALIFIER}-{role_type}-role-{self.account}-{self.region}"
            for role_type in CDK_BOOTSTRAP_ROLE_TYPES
        ]
        assume_cdk_roles = aws_iam.PolicyDocument(
            statements=[
                aws_iam.PolicyStatement(
                    actions=["sts:AssumeRole"],
                    resources=cdk_role_arns,
                )
            ]
        )
        self.deployment_role = aws_iam.Role(
            self,
            "ValkyrieDeploymentRole",
            role_name="ValkyrieDevGitHubDeploymentRole",
            assumed_by=cast(
                aws_iam.IPrincipal,
                aws_iam.FederatedPrincipal(
                    federated=self.oidc_provider.attr_arn,
                    conditions={
                        "StringEquals": {
                            f"{GITHUB_OIDC_ISSUER}:aud": GITHUB_OIDC_AUDIENCE,
                            f"{GITHUB_OIDC_ISSUER}:sub": GITHUB_DEV_SUBJECT,
                            f"{GITHUB_OIDC_ISSUER}:repository_id": GITHUB_REPOSITORY_ID,
                            f"{GITHUB_OIDC_ISSUER}:repository_owner_id": GITHUB_REPOSITORY_OWNER_ID,
                            f"{GITHUB_OIDC_ISSUER}:ref": GITHUB_DEV_REF,
                            f"{GITHUB_OIDC_ISSUER}:environment": GITHUB_DEV_ENVIRONMENT,
                        }
                    },
                    assume_role_action="sts:AssumeRoleWithWebIdentity",
                ),
            ),
            inline_policies={"AssumeCdkBootstrapRoles": assume_cdk_roles},
        )

        aws_ssm.StringParameter(
            self,
            "GitHubOidcProviderArnParameter",
            parameter_name=OIDC_PROVIDER_ARN_PARAMETER,
            string_value=self.oidc_provider.attr_arn,
        )
        aws_ssm.StringParameter(
            self,
            "ValkyrieRoleArnParameter",
            parameter_name=VALKYRIE_ROLE_ARN_PARAMETER,
            string_value=self.deployment_role.role_arn,
        )
