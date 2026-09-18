"""Fail-closed execution reference checks without secret-value access."""

import hashlib
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer.contracts import StoredReference
from tracker.run_transfer.rows import RowClosure, digest


def verify_portable_references(rows: RowClosure, session: Any, account: str, region: str) -> str:
    benchmark = rows.rows["benchmark"][0]
    arguments = benchmark["arguments"]
    proofs: list[dict[str, Any]] = []

    def secret(value: Any, pointer: str) -> None:
        if value is None:
            return
        if not isinstance(value, str) or not value:
            raise LifecycleConflict("Unknown secret reference format")
        response = session.client("secretsmanager", region_name=region).describe_secret(SecretId=value)
        if (
            not response.get("ARN", "").startswith(f"arn:aws:secretsmanager:{region}:{account}:secret:")
            or response.get("DeletedDate") is not None
            or not any("AWSCURRENT" in stages for stages in response.get("VersionIdsToStages", {}).values())
        ):
            raise LifecycleConflict("Destination secret metadata is unresolved")
        proofs.append(
            {
                "pointer": pointer,
                "value_sha256": digest(value),
                "metadata_sha256": digest({"arn": response["ARN"], "versions": response["VersionIdsToStages"]}),
            }
        )

    def object_reference(value: str, pointer: str) -> None:
        parsed = urlsplit(value)
        query = parse_qs(parsed.query)
        if (
            parsed.scheme != "s3"
            or not parsed.netloc
            or not parsed.path.lstrip("/")
            or parsed.fragment
            or set(query) != {"versionId"}
            or len(query["versionId"]) != 1
            or query["versionId"][0] in {"", "null"}
        ):
            raise LifecycleConflict("Executable object requires an exact immutable destination version")
        response = session.client("s3", region_name=region).get_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
            VersionId=query["versionId"][0],
            ExpectedBucketOwner=account,
        )
        body = response["Body"]
        try:
            checksum = hashlib.sha256()
            size = 0
            while chunk := body.read(1024 * 1024):
                checksum.update(chunk)
                size += len(chunk)
        finally:
            body.close()
        if response.get("VersionId") != query["versionId"][0] or response.get("ContentLength") != size:
            raise LifecycleConflict("Executable destination version differs")
        proofs.append(
            {
                "pointer": pointer,
                "value_sha256": digest(value),
                "sha256": checksum.hexdigest(),
                "version_id": response["VersionId"],
            }
        )

    def scan(value: Any, pointer: str) -> None:
        if isinstance(value, dict):
            for name, child in cast(dict[str, Any], value).items():
                child_pointer = pointer + "/" + name.replace("~", "~0").replace("/", "~1")
                if child_pointer == "/arguments/contract/secrets":
                    if not isinstance(child, dict):
                        raise LifecycleConflict("Unknown contract secret map format")

                    for environment_name, locator in cast(dict[str, Any], child).items():
                        if not isinstance(locator, str):
                            raise LifecycleConflict("Unknown contract secret locator format")

                        secret(locator, child_pointer + "/" + environment_name.replace("~", "~0").replace("/", "~1"))
                elif "secret" in name.lower():
                    if isinstance(child, list):
                        for index, item in enumerate(cast(list[Any], child)):
                            secret(item, child_pointer + "/" + str(index))
                    else:
                        secret(child, child_pointer)
                else:
                    scan(child, child_pointer)
        elif isinstance(value, list):
            for index, child in enumerate(cast(list[Any], value)):
                scan(child, pointer + "/" + str(index))
        elif isinstance(value, str):
            if value.startswith("s3://"):
                object_reference(value, pointer)
            elif any(marker in value for marker in ("http:", "https:", "arn:", "s3:", "file:", "/")):
                raise LifecycleConflict("Unknown executable locator requires history_only")

    if benchmark["custom_benchmark_service"] is not None or arguments.get("lambda_function") is not None:
        raise LifecycleConflict("Callback or custom service portability is unresolved")
    dataset = arguments.get("dataset")
    if dataset is not None:
        if not isinstance(dataset, str) or not dataset.startswith("s3://"):
            raise LifecycleConflict("Dataset portability is unresolved")
        object_reference(dataset, "/arguments/dataset")
    secret(benchmark["webhook_secret_name"], "/webhook_secret_name")
    secret(arguments.get("sandbox_provider_secret_name"), "/arguments/sandbox_provider_secret_name")
    scan(arguments.get("contract"), "/arguments/contract")
    return digest(proofs)


def inventory_references(rows: RowClosure) -> tuple[StoredReference, ...]:
    result: list[StoredReference] = []
    for table, values in rows.rows.items():
        for row in values:

            def visit(value: Any, pointer: str, key: str) -> None:
                if isinstance(value, dict):
                    for name, child in cast(dict[str, Any], value).items():
                        visit(
                            child,
                            pointer + "/" + name.replace("~", "~0").replace("/", "~1"),
                            "secrets" if table == "benchmark" and pointer == "/arguments/contract/secrets" else name,
                        )
                elif isinstance(value, list):
                    for index, child in enumerate(cast(list[Any], value)):
                        visit(child, pointer + "/" + str(index), key)
                elif isinstance(value, str) and value:
                    kind = (
                        "secret_locator"
                        if "secret" in key.lower()
                        else "external_locator"
                        if any(marker in value for marker in ("s3://", "https://", "http://", "arn:"))
                        else "execution_input"
                        if key in {"dataset", "lambda_function", "custom_benchmark_service"}
                        else None
                    )
                    if kind is not None:
                        result.append(
                            StoredReference.model_validate(
                                {
                                    "table": table,
                                    "row_id": row["id"],
                                    "pointer": pointer,
                                    "kind": kind,
                                    "value_sha256": digest(value),
                                }
                            )
                        )

            visit(row, "", "")
    return tuple(result)
