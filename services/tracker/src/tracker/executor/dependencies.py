"""Dependencies scoped to one executor dispatch."""

from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, Org
from tracker.exceptions import TrackerServiceError
from tracker.runtime.services import RuntimeServices
from tracker.types import StartBenchmarkRequest


async def get_execution_runtime(
    request: StartBenchmarkRequest,
    benchmark: Benchmark,
    org: Org,
    *,
    context_version: int | None = None,
) -> RuntimeServices:
    stored = benchmark.arguments.properties
    queued = request.properties
    if context_version == 3 and stored is None:
        raise TrackerServiceError("Managed execution has no saved AWS resources")

    if stored is not None and (queued is not None or context_version == 3) and queued != stored:
        raise TrackerServiceError("Queued AWS resources differ from the saved run")

    if context_version == 2:
        resources = stored or queued
        if resources is not None and resources.s3_bucket.startswith(("vs-dev-", "vs-prod-")):
            raise TrackerServiceError("Protocol 2 cannot execute owner storage")

    return await CloudRuntimeFactory.create_execution_runtime(
        request, org.id, benchmark.id, properties=stored, context_version=context_version
    )
