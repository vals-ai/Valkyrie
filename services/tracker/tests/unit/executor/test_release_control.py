from datetime import UTC, datetime, timedelta
import hashlib
import io
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
)
from tracker.executor.release_control import (
    ReleaseControlError,
    activate_release,
    active_executor_release_work,
    artifact_deletion_allowed,
    create_executor_dispatch,
    promote_release,
    register_release,
    retire_drained_releases,
    select_active_release,
    verify_release_artifact,
)


def _release(release_id: str) -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )


def _dispatch(
    benchmark_id: UUID,
    release: ExecutorRelease,
    kind: ExecutorDispatchKind,
) -> ExecutorDispatch:
    dispatch_id = uuid4()
    return create_executor_dispatch(
        benchmark_id,
        release,
        kind,
        dispatch_id=dispatch_id,
    )


class FakeS3Client:
    def __init__(self, content: bytes, *, key: str = "v1.pex") -> None:
        self.content = content
        self.key = key

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        assert (Bucket, Key) == ("artifacts", self.key)
        return {"Body": io.BytesIO(self.content)}


def test_activate_release_registers_verifies_and_promotes_in_one_session(database_session: Session) -> None:
    content = b"executor artifact"
    release = ExecutorRelease(
        id="git-abc123-def456",
        artifact_uri="s3://artifacts/releases/git-abc123-def456/executor.pex",
        artifact_digest=hashlib.sha256(content).hexdigest(),
        protocol_version="1",
    )

    activated = activate_release(
        database_session,
        release,
        expected_bucket="artifacts",
        expected_prefix="releases",
        s3_client=FakeS3Client(content, key="releases/git-abc123-def456/executor.pex"),
    )

    admission = database_session.get(ExecutorAdmission, 1)
    assert activated.status == ExecutorReleaseStatus.ACTIVE
    assert activated.readiness_verified
    assert admission is not None
    assert admission.release_id == release.id


def test_activate_release_is_idempotent_for_exact_active_release(database_session: Session) -> None:
    content = b"executor artifact"
    release = ExecutorRelease(
        id="git-abc123-def456",
        artifact_uri="s3://artifacts/releases/git-abc123-def456/executor.pex",
        artifact_digest=hashlib.sha256(content).hexdigest(),
        protocol_version="1",
    )
    client = FakeS3Client(content, key="releases/git-abc123-def456/executor.pex")
    first = activate_release(
        database_session,
        release,
        expected_bucket="artifacts",
        expected_prefix="releases",
        s3_client=client,
    )
    activated_at = first.activated_at

    second = activate_release(
        database_session,
        ExecutorRelease(
            id=release.id,
            artifact_uri=release.artifact_uri,
            artifact_digest=release.artifact_digest,
            protocol_version=release.protocol_version,
        ),
        expected_bucket="artifacts",
        expected_prefix="releases",
        s3_client=client,
    )

    assert second.status == ExecutorReleaseStatus.ACTIVE
    assert second.activated_at is not None
    assert activated_at is not None
    assert second.activated_at.replace(tzinfo=None) == activated_at.replace(tzinfo=None)


def test_activate_release_rejects_draining_release(database_session: Session) -> None:
    content = b"executor artifact"
    previous = ExecutorRelease(
        id="git-previous",
        artifact_uri="s3://artifacts/releases/git-previous/executor.pex",
        artifact_digest=hashlib.sha256(content).hexdigest(),
        protocol_version="1",
    )
    current = ExecutorRelease(
        id="git-current",
        artifact_uri="s3://artifacts/releases/git-current/executor.pex",
        artifact_digest=hashlib.sha256(content).hexdigest(),
        protocol_version="1",
    )
    activate_release(
        database_session,
        previous,
        expected_bucket="artifacts",
        expected_prefix="releases",
        s3_client=FakeS3Client(content, key="releases/git-previous/executor.pex"),
    )
    activate_release(
        database_session,
        current,
        expected_bucket="artifacts",
        expected_prefix="releases",
        s3_client=FakeS3Client(content, key="releases/git-current/executor.pex"),
    )

    with pytest.raises(ReleaseControlError, match="cannot be activated from DRAINING"):
        activate_release(
            database_session,
            ExecutorRelease(
                id=previous.id,
                artifact_uri=previous.artifact_uri,
                artifact_digest=previous.artifact_digest,
                protocol_version=previous.protocol_version,
            ),
            expected_bucket="artifacts",
            expected_prefix="releases",
            s3_client=FakeS3Client(content, key="releases/git-previous/executor.pex"),
        )


def test_activate_release_rejects_retired_release(database_session: Session) -> None:
    content = b"executor artifact"
    release = ExecutorRelease(
        id="git-retired",
        artifact_uri="s3://artifacts/releases/git-retired/executor.pex",
        artifact_digest=hashlib.sha256(content).hexdigest(),
        protocol_version="1",
    )
    register_release(database_session, release)
    release.status = ExecutorReleaseStatus.RETIRED
    database_session.add(release)
    database_session.flush()

    with pytest.raises(ReleaseControlError, match="cannot be activated from RETIRED"):
        activate_release(
            database_session,
            ExecutorRelease(
                id=release.id,
                artifact_uri=release.artifact_uri,
                artifact_digest=release.artifact_digest,
                protocol_version=release.protocol_version,
            ),
            expected_bucket="artifacts",
            expected_prefix="releases",
            s3_client=FakeS3Client(content, key="releases/git-retired/executor.pex"),
        )


def test_activate_release_rejects_release_id_reuse_with_different_content(database_session: Session) -> None:
    existing = ExecutorRelease(
        id="git-abc123-def456",
        artifact_uri="s3://artifacts/releases/git-abc123-def456/executor.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
    )
    register_release(database_session, existing)

    with pytest.raises(ReleaseControlError, match="different immutable identity"):
        activate_release(
            database_session,
            ExecutorRelease(
                id=existing.id,
                artifact_uri=existing.artifact_uri,
                artifact_digest="b" * 64,
                protocol_version=existing.protocol_version,
            ),
            expected_bucket="artifacts",
            expected_prefix="releases",
            s3_client=FakeS3Client(b"irrelevant"),
        )


def test_activate_release_digest_failure_rolls_back_new_candidate(database_session: Session) -> None:
    release = ExecutorRelease(
        id="git-abc123-def456",
        artifact_uri="s3://artifacts/releases/git-abc123-def456/executor.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
    )

    with pytest.raises(ReleaseControlError, match="digest mismatch"):
        activate_release(
            database_session,
            release,
            expected_bucket="artifacts",
            expected_prefix="releases",
            s3_client=FakeS3Client(b"different", key="releases/git-abc123-def456/executor.pex"),
        )
    database_session.rollback()

    assert database_session.get(ExecutorRelease, release.id) is None


def test_activate_release_rejects_artifact_outside_configured_location(database_session: Session) -> None:
    release = _release("git-abc123-def456")

    with pytest.raises(ReleaseControlError, match="configured S3 bucket and prefix"):
        activate_release(
            database_session,
            release,
            expected_bucket="release-artifacts",
            expected_prefix="releases",
            s3_client=FakeS3Client(b"irrelevant"),
        )


def test_verify_release_artifact_marks_candidate_ready(database_session: Session) -> None:
    content = b"executor artifact"
    release = _release("v1")
    release.artifact_digest = hashlib.sha256(content).hexdigest()
    register_release(database_session, release)

    verified = verify_release_artifact(database_session, "v1", s3_client=FakeS3Client(content))

    assert verified.readiness_verified
    assert verified.readiness_metadata["artifact_bytes"] == len(content)


def test_verify_release_artifact_rejects_digest_mismatch(database_session: Session) -> None:
    release = _release("v1")
    release.readiness_verified = False
    register_release(database_session, release)

    with pytest.raises(ReleaseControlError, match="digest mismatch"):
        verify_release_artifact(database_session, "v1", s3_client=FakeS3Client(b"different artifact"))

    stored = database_session.get(ExecutorRelease, "v1")
    assert stored is not None
    assert not stored.readiness_verified


def test_register_release_rejects_unsupported_protocol(database_session: Session) -> None:
    release = _release("unsupported")
    release.protocol_version = "3"

    with pytest.raises(ReleaseControlError, match="Unsupported executor protocol version"):
        register_release(database_session, release)


def test_register_release_rejects_invalid_digest(database_session: Session) -> None:
    release = _release("invalid-digest")
    release.artifact_digest = "not-a-digest"

    with pytest.raises(ReleaseControlError, match="SHA-256"):
        register_release(database_session, release)


def test_promote_release_requires_readiness(database_session: Session) -> None:
    release = _release("unready")
    release.readiness_verified = False
    register_release(database_session, release)

    with pytest.raises(ReleaseControlError, match="is not ready"):
        promote_release(database_session, "unready")


def test_promote_release_is_idempotent_for_current_release(database_session: Session) -> None:
    register_release(database_session, _release("v1"))

    first = promote_release(database_session, "v1")
    activated_at = first.activated_at
    second = promote_release(database_session, "v1")

    assert activated_at is not None
    assert second.activated_at is not None
    assert second.activated_at.replace(tzinfo=None) == activated_at.replace(tzinfo=None)


def test_promote_release_refreshes_cached_admission_and_previous_release(
    database_session: Session,
) -> None:
    database_session.add_all([_release("v1"), _release("v2"), _release("v3")])
    database_session.commit()
    promote_release(database_session, "v1")
    database_session.commit()

    first_session = Session(bind=database_session.bind)
    second_session = Session(bind=database_session.bind)
    final_session = Session(bind=database_session.bind)
    try:
        assert first_session.get(ExecutorAdmission, 1) is not None
        for release_id in ("v1", "v2", "v3"):
            assert first_session.get(ExecutorRelease, release_id) is not None

        promote_release(second_session, "v2")
        second_session.commit()
        promote_release(first_session, "v3")
        first_session.commit()

        statuses = {}
        for release_id in ("v1", "v2", "v3"):
            release = final_session.get(ExecutorRelease, release_id)
            assert release is not None
            statuses[release_id] = release.status
        assert statuses == {
            "v1": ExecutorReleaseStatus.DRAINING,
            "v2": ExecutorReleaseStatus.DRAINING,
            "v3": ExecutorReleaseStatus.ACTIVE,
        }
    finally:
        first_session.close()
        second_session.close()
        final_session.close()


def test_promote_release_moves_previous_admission_to_draining(database_session: Session) -> None:
    register_release(database_session, _release("v1"))
    register_release(database_session, _release("v2"))

    promote_release(database_session, "v1")
    promote_release(database_session, "v2")

    active = select_active_release(database_session)
    assert active.id == "v2"
    previous = database_session.get(ExecutorRelease, "v1")
    assert previous is not None
    assert previous.status == ExecutorReleaseStatus.DRAINING


def test_promote_release_rejects_draining_release(database_session: Session) -> None:
    register_release(database_session, _release("v1"))
    register_release(database_session, _release("v2"))
    promote_release(database_session, "v1")
    promote_release(database_session, "v2")

    with pytest.raises(ReleaseControlError, match="cannot be promoted from DRAINING"):
        promote_release(database_session, "v1")


def test_select_active_release_requires_an_admission_target(database_session: Session) -> None:
    with pytest.raises(ReleaseControlError, match="No active executor release"):
        select_active_release(database_session)


@pytest.mark.parametrize(
    ("status", "readiness_verified"),
    [
        (ExecutorReleaseStatus.CANDIDATE, True),
        (ExecutorReleaseStatus.DRAINING, True),
        (ExecutorReleaseStatus.ACTIVE, False),
    ],
)
def test_select_active_release_rejects_invalid_admission_target(
    database_session: Session,
    status: ExecutorReleaseStatus,
    readiness_verified: bool,
) -> None:
    release = _release("invalid")
    release.status = status
    release.readiness_verified = readiness_verified
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    admission.release_id = release.id
    database_session.add(release)
    database_session.add(admission)
    database_session.commit()

    with pytest.raises(ReleaseControlError, match="No active executor release"):
        select_active_release(database_session)


def test_promote_release_does_not_backfill_legacy_benchmark_ownership(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    terminal_benchmark = Benchmark(
        id=uuid4(),
        org_id=example_benchmark_object.org_id,
        name=example_benchmark_object.name,
        status=BenchmarkStatus.FINISHED,
        finished_at=datetime.now(UTC),
        arguments=example_benchmark_object.arguments.model_copy(deep=True),
    )
    database_session.add(example_benchmark_object)
    database_session.add(terminal_benchmark)
    register_release(database_session, _release("legacy"))
    register_release(database_session, _release("unrelated"))
    database_session.commit()

    promote_release(database_session, "legacy")
    promote_release(database_session, "unrelated")
    database_session.commit()

    database_session.refresh(example_benchmark_object)
    database_session.refresh(terminal_benchmark)
    assert example_benchmark_object.executor_release_id is None
    assert example_benchmark_object.current_execution_release_id is None
    assert terminal_benchmark.executor_release_id is None
    assert terminal_benchmark.current_execution_release_id is None

    assert retire_drained_releases(database_session) == []
    legacy = database_session.get(ExecutorRelease, "legacy")
    assert legacy is not None
    assert legacy.status == ExecutorReleaseStatus.DRAINING


def test_retirement_rejects_the_current_admission_target(database_session: Session) -> None:
    register_release(database_session, _release("v1"))
    promote_release(database_session, "v1")
    release = database_session.get(ExecutorRelease, "v1")
    assert release is not None
    release.status = ExecutorReleaseStatus.DRAINING
    database_session.add(release)
    database_session.commit()

    with pytest.raises(ReleaseControlError, match="active admission target cannot be draining"):
        retire_drained_releases(database_session)


def test_partial_benchmark_ownership_is_rejected(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("v1"))
    example_benchmark_object.executor_release_id = "v1"
    database_session.add(example_benchmark_object)

    with pytest.raises(IntegrityError):
        database_session.commit()
    database_session.rollback()


def test_retirement_waits_for_owned_active_benchmark(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("v1"))
    register_release(database_session, _release("v2"))
    promote_release(database_session, "v1")
    promote_release(database_session, "v2")

    benchmark = example_benchmark_object
    benchmark.executor_release_id = "v1"
    benchmark.executor_artifact_uri = "s3://artifacts/v1.pex"
    benchmark.executor_artifact_digest = "digest-v1"
    benchmark.executor_protocol_version = "1"
    database_session.add(benchmark)
    database_session.commit()

    assert retire_drained_releases(database_session) == []

    benchmark.status = BenchmarkStatus.FINISHED
    database_session.add(benchmark)
    database_session.commit()

    assert retire_drained_releases(database_session) == ["v1"]
    retired = database_session.get(ExecutorRelease, "v1")
    assert retired is not None
    assert retired.status == ExecutorReleaseStatus.RETIRED
    assert retired.artifact_retention_until is not None
    retention_until = retired.artifact_retention_until.replace(tzinfo=UTC)
    assert not artifact_deletion_allowed(
        database_session,
        "v1",
        now=retention_until - timedelta(seconds=1),
    )
    assert artifact_deletion_allowed(database_session, "v1", now=retention_until)
    with pytest.raises(ReleaseControlError, match="cannot be promoted from RETIRED"):
        promote_release(database_session, "v1")


def test_active_work_deduplicates_current_owner_and_dispatch_on_the_same_release(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = register_release(database_session, _release("v1"))
    promote_release(database_session, release.id)
    benchmark = example_benchmark_object
    benchmark.executor_release_id = release.id
    benchmark.current_execution_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version
    database_session.add(benchmark)
    database_session.flush()
    dispatch = _dispatch(benchmark.id, release, ExecutorDispatchKind.RETRY)
    dispatch.status = ExecutorDispatchStatus.RUNNING
    database_session.add(dispatch)
    database_session.commit()

    assert active_executor_release_work(database_session).counts_by_release == {release.id: 1}


def test_null_current_owner_with_start_dispatch_history_blocks_every_retirement(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release_a = register_release(database_session, _release("v1"))
    release_b = register_release(database_session, _release("v2"))
    promote_release(database_session, release_a.id)
    promote_release(database_session, release_b.id)
    benchmark = example_benchmark_object
    benchmark.executor_release_id = release_a.id
    benchmark.current_execution_release_id = None
    benchmark.executor_artifact_uri = release_a.artifact_uri
    benchmark.executor_artifact_digest = release_a.artifact_digest
    benchmark.executor_protocol_version = release_a.protocol_version
    database_session.add(benchmark)
    database_session.flush()
    dispatch = _dispatch(benchmark.id, release_a, ExecutorDispatchKind.START)
    dispatch.status = ExecutorDispatchStatus.FINISHED
    dispatch.finished_at = datetime.now(UTC)
    database_session.add(dispatch)
    database_session.commit()

    assert retire_drained_releases(database_session) == []
    assert release_a.status == ExecutorReleaseStatus.DRAINING


@pytest.mark.parametrize(
    "dispatch_status",
    [ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING],
)
def test_retirement_waits_for_active_dispatch(
    database_session: Session,
    example_benchmark_object: Benchmark,
    dispatch_status: ExecutorDispatchStatus,
) -> None:
    release_v1 = register_release(database_session, _release("v1"))
    register_release(database_session, _release("v2"))
    promote_release(database_session, release_v1.id)
    promote_release(database_session, "v2")

    example_benchmark_object.status = BenchmarkStatus.FINISHED
    example_benchmark_object.finished_at = datetime.now(UTC)
    database_session.add(example_benchmark_object)
    database_session.flush()
    dispatch = _dispatch(example_benchmark_object.id, release_v1, ExecutorDispatchKind.START)
    dispatch.status = dispatch_status
    database_session.add(dispatch)
    database_session.flush()

    assert retire_drained_releases(database_session) == []
    assert release_v1.status == ExecutorReleaseStatus.DRAINING


def test_retire_drained_releases_retires_only_releases_without_active_work(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    for release_id in ("v1", "v2", "v3", "candidate"):
        register_release(database_session, _release(release_id))
    promote_release(database_session, "v1")
    promote_release(database_session, "v2")
    promote_release(database_session, "v3")

    release_v2 = database_session.get(ExecutorRelease, "v2")
    assert release_v2 is not None
    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    example_benchmark_object.current_execution_release_id = release_v2.id
    database_session.add(example_benchmark_object)
    database_session.flush()

    retired = retire_drained_releases(database_session)

    release_v1 = database_session.get(ExecutorRelease, "v1")
    assert release_v1 is not None
    candidate = database_session.get(ExecutorRelease, "candidate")
    assert candidate is not None
    assert retired == ["v1"]
    assert release_v1.status == ExecutorReleaseStatus.RETIRED
    assert release_v2.status == ExecutorReleaseStatus.DRAINING
    assert candidate.status == ExecutorReleaseStatus.CANDIDATE
    assert retire_drained_releases(database_session) == []


def test_retire_drained_releases_fails_closed_for_unattributed_active_work(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    for release_id in ("v1", "v2"):
        register_release(database_session, _release(release_id))
    promote_release(database_session, "v1")
    promote_release(database_session, "v2")

    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    example_benchmark_object.current_execution_release_id = None
    database_session.add(example_benchmark_object)
    database_session.flush()

    assert retire_drained_releases(database_session) == []
    release_v1 = database_session.get(ExecutorRelease, "v1")
    assert release_v1 is not None
    assert release_v1.status == ExecutorReleaseStatus.DRAINING


def test_artifact_deletion_allowed_treats_naive_retention_timestamp_as_utc(database_session: Session) -> None:
    retention_until = datetime(2026, 8, 19)
    release = _release("v1")
    release.status = ExecutorReleaseStatus.RETIRED
    release.artifact_retention_until = retention_until
    database_session.add(release)
    database_session.commit()

    assert not artifact_deletion_allowed(
        database_session,
        release.id,
        now=retention_until.replace(tzinfo=UTC) - timedelta(seconds=1),
    )
    assert artifact_deletion_allowed(
        database_session,
        release.id,
        now=retention_until.replace(tzinfo=UTC),
    )


def test_active_retry_dispatch_blocks_its_release_across_successive_promotions(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    for release_id in ("v1", "v2", "v3"):
        register_release(database_session, _release(release_id))
    promote_release(database_session, "v1")

    benchmark = example_benchmark_object
    benchmark.executor_release_id = "v1"
    benchmark.executor_artifact_uri = "s3://artifacts/v1.pex"
    benchmark.executor_artifact_digest = "a" * 64
    benchmark.executor_protocol_version = "1"
    benchmark.status = BenchmarkStatus.FINISHED
    benchmark.finished_at = datetime.now(UTC)
    database_session.add(benchmark)
    database_session.commit()

    promote_release(database_session, "v2")
    retry_dispatch = _dispatch(
        benchmark.id,
        select_active_release(database_session),
        ExecutorDispatchKind.RETRY,
    )
    retry_dispatch.status = ExecutorDispatchStatus.RUNNING
    retry_dispatch.started_at = datetime.now(UTC)
    database_session.add(retry_dispatch)
    database_session.commit()

    promote_release(database_session, "v3")

    assert retire_drained_releases(database_session) == ["v1"]

    stored_dispatch = database_session.get(ExecutorDispatch, retry_dispatch.id)
    assert stored_dispatch is not None
    stored_dispatch.status = ExecutorDispatchStatus.FINISHED
    stored_dispatch.finished_at = datetime.now(UTC)
    database_session.add(stored_dispatch)
    database_session.commit()

    assert retire_drained_releases(database_session) == ["v2"]
