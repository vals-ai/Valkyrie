from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def unit_test_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DAYTONA_API_KEY", "test_key")
    monkeypatch.setenv("DAYTONA_API_URL", "http://test.url")
    monkeypatch.setenv("DAYTONA_TARGET", "test_target")


@pytest.fixture(autouse=True)
def mock_aws_services(monkeypatch: pytest.MonkeyPatch):
    def create_benchmark_group(benchmark_id: str) -> str:
        return f"mock-group-{benchmark_id}"

    def cloudwatch_stream(_stream_key: str, _message: str) -> None:
        pass

    monkeypatch.setattr("tracker.cloudwatch.create_benchmark_group", create_benchmark_group)
    monkeypatch.setattr("tracker.cloudwatch.cloudwatch_stream", cloudwatch_stream)

    mock_s3_download = MagicMock(return_value=b"mock-contract-content")
    monkeypatch.setattr("tracker.s3.download_from_s3", mock_s3_download)

    def get_contract_s3_key(contract_name: str) -> str:
        return f"contracts/{contract_name}.zip"

    monkeypatch.setattr("tracker.s3.get_contract_s3_key", get_contract_s3_key)
