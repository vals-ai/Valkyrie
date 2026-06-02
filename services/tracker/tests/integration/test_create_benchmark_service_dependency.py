"""Integration tests for pinned create-benchmark-service sandbox provider behavior."""

from benchmark_service import ImageSource, Resources, Sandbox, SandboxCreateRequest, SandboxProvider


def _request(name: str, source: ImageSource, resources: Resources) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source,
        resources=resources,
        name=name,
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )


async def test_daytona_provider_dependency_recreates_name_after_delete(
    sandbox_provider: SandboxProvider,
    test_resources: Resources,
    test_image: str,
    random_sandbox_name: str,
) -> None:
    """
    Tracker depends on create-benchmark-service to wait when Daytona keeps a deleted sandbox name reserved.

    Test cases:
    - A real Daytona sandbox can be deleted and recreated immediately with the same name.
    - The recreated sandbox is usable and is cleaned up through the provider API.
    """
    source = ImageSource(image=test_image)
    request = _request(random_sandbox_name, source, test_resources)
    recreated_sandbox: Sandbox | None = None

    first_sandbox = await sandbox_provider.create_sandbox(request)
    await sandbox_provider.delete_sandbox(first_sandbox.id)

    try:
        recreated_sandbox = await sandbox_provider.create_sandbox(request)
        result = await recreated_sandbox.exec("echo recreated")

        assert recreated_sandbox.name == random_sandbox_name
        assert result.exit_code == 0
        assert result.output.strip() == "recreated"
    finally:
        if recreated_sandbox is not None:
            await sandbox_provider.delete_sandbox(recreated_sandbox.id)
