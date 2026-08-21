import io

from tracker.aws.executor_artifacts import S3ExecutorArtifactReader


class _S3Client:
    def __init__(self, body: io.BytesIO) -> None:
        self.body = body
        self.calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        self.calls.append((Bucket, Key))
        return {"Body": self.body}


def test_s3_executor_artifact_reader_closes_stream_after_read() -> None:
    body = io.BytesIO(b"sealed executor")
    client = _S3Client(body)

    with S3ExecutorArtifactReader(client).open("artifacts", "releases/v1/executor.pex") as stream:
        assert stream.read() == b"sealed executor"

    assert client.calls == [("artifacts", "releases/v1/executor.pex")]
    assert body.closed
