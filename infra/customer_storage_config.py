"""Explicit inputs for the production customer-storage boundary; no cloud lookups."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from stage import Stage


@dataclass(frozen=True)
class CustomerStorageConfig:
    account_id: str
    organization_id: str
    oidc_provider_arn: str
    oidc_audience: str
    oidc_subject: str
    lambda_name: str
    operator_role_arn: str
    legacy_bucket: str
    legacy_account_id: str

    @classmethod
    def from_environment(
        cls, stage: Stage, account_id: str, environment: Mapping[str, str]
    ) -> "CustomerStorageConfig | None":
        enabled = environment.get("VALSMITH_CUSTOMER_STORAGE_ENABLED", "false")
        if enabled not in ("true", "false"):
            raise ValueError("VALSMITH_CUSTOMER_STORAGE_ENABLED must be true or false")

        if enabled == "false":
            return None

        if stage.name != "prod":
            raise ValueError("VALSMITH_CUSTOMER_STORAGE_ENABLED requires stage prod")

        def required(name: str) -> str:
            value = environment.get(name, "")
            if not value or value != value.strip():
                raise ValueError(f"{name} must be explicit and nonempty, without surrounding whitespace")

            return value

        production_account = required("PRODUCTION_ACCOUNT_ID")
        if re.fullmatch(r"[0-9]{12}", production_account) is None or production_account != account_id:
            raise ValueError("PRODUCTION_ACCOUNT_ID must be the explicit 12-digit CDK target account")

        for name in ("BENCH_ACCOUNT_ID", "DEV_ACCOUNT_ID"):
            if environment.get(name) == production_account:
                raise ValueError(f"PRODUCTION_ACCOUNT_ID must differ from {name}")

        organization = required("VALSMITH_STORAGE_ORG_ID")
        try:
            parsed_organization = UUID(organization)
        except ValueError:
            raise ValueError("VALSMITH_STORAGE_ORG_ID must be a canonical nonzero UUID") from None

        if str(parsed_organization) != organization or parsed_organization.int == 0:
            raise ValueError("VALSMITH_STORAGE_ORG_ID must be a canonical nonzero UUID")

        provider = required("VALSMITH_STORAGE_OIDC_PROVIDER_ARN")
        if provider != f"arn:aws:iam::{production_account}:oidc-provider/oidc.vercel.com/vals-ai":
            raise ValueError("VALSMITH_STORAGE_OIDC_PROVIDER_ARN must name the vals-ai provider in the target account")

        audience = required("VALSMITH_STORAGE_OIDC_AUDIENCE")
        if audience != "https://vercel.com/vals-ai":
            raise ValueError("VALSMITH_STORAGE_OIDC_AUDIENCE must be https://vercel.com/vals-ai")

        subject = required("VALSMITH_STORAGE_OIDC_SUBJECT")
        if subject != "owner:vals-ai:project:valsmith:environment:production":
            raise ValueError("VALSMITH_STORAGE_OIDC_SUBJECT must select vals-ai/valsmith production exactly")

        lambda_name = required("VALSMITH_DATASET_VIEW_LAMBDA_NAME")
        if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", lambda_name) is None:
            raise ValueError("VALSMITH_DATASET_VIEW_LAMBDA_NAME must be one literal Lambda function name")

        operator = required("VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN")
        if re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+", operator) is None:
            raise ValueError("VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN must be one exact IAM role ARN")

        runtime_role_names = {
            "ValSmithStorage-prod",
            "ValSmithDatasetView-prod",
            "ValSmithBackup-prod",
            "ValSmithLifecycle-prod",
            "ValkyrieTrackerTaskRole-prod",
            "ValkyrieExecutorTaskRole-prod",
        }
        if operator.rsplit("/", 1)[-1] in runtime_role_names:
            raise ValueError("VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN must be a separate operator role")

        legacy_bucket = required("VALSMITH_LEGACY_STORAGE_BUCKET")
        if re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", legacy_bucket) is None or legacy_bucket.startswith("vs-"):
            raise ValueError("VALSMITH_LEGACY_STORAGE_BUCKET must be an exact shared bucket, not an owner bucket")

        legacy_account = required("VALSMITH_LEGACY_STORAGE_ACCOUNT_ID")
        if re.fullmatch(r"[0-9]{12}", legacy_account) is None:
            raise ValueError("VALSMITH_LEGACY_STORAGE_ACCOUNT_ID must be an explicit 12-digit account")

        return cls(
            production_account,
            organization,
            provider,
            audience,
            subject,
            lambda_name,
            operator,
            legacy_bucket,
            legacy_account,
        )
