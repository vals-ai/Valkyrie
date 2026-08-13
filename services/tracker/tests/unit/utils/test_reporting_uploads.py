from typing import Any, cast
from uuid import uuid4

import pytest

import tracker.utils.reporting as reporting_module
from tracker.database.models import Benchmark
from tracker.types import FinalViewResponse, HarnessConfig


class _SerializableFinalView:
    def model_dump_json(self, **_kwargs: object) -> str:
        return '{"status":"FINISHED"}'


@pytest.mark.asyncio
async def test_final_view_upload_is_verified_before_returning(
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    benchmark_id = uuid4()
    benchmark = cast(Benchmark, type("BenchmarkStub", (), {"id": benchmark_id, "name": "valsmith"})())
    final_view = cast(FinalViewResponse, _SerializableFinalView())
    uploaded: list[tuple[bytes, str]] = []
    verified: list[tuple[str, str, int, str]] = []

    async def upload(content: bytes, key: str, *_args: Any) -> str:
        uploaded.append((content, key))
        return '"uploaded-etag"'

    async def verify(
        key: str,
        _aws: object,
        bucket: str,
        *,
        expected_size: int,
        expected_etag: str,
    ) -> None:
        verified.append((bucket, key, expected_size, expected_etag))

    monkeypatch.setattr(reporting_module, "upload_to_s3", upload)
    monkeypatch.setattr(reporting_module, "verify_s3_object", verify, raising=False)

    key = await reporting_module.upload_final_view(benchmark, final_view, harness_config)

    expected_content = b'{"status":"FINISHED"}'
    assert uploaded == [(expected_content, key)]
    assert verified == [(harness_config.s3_bucket, key, len(expected_content), '"uploaded-etag"')]
