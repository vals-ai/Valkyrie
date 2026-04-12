"""Test whether a sandbox with no idle timeout stays alive after a command finishes.

Setup: Create a sandbox with auto_stop_interval=0 (disabled),
run a short 30s sleep, then monitor sandbox state for ~3 minutes after it completes.

Fetches Daytona credentials from AWS Secrets Manager.
"""

import asyncio
import json
import os
import time
import uuid

import boto3
from daytona import (
    AsyncDaytona,
    CreateSandboxFromSnapshotParams,
    DaytonaConfig,
    SandboxState,
    SessionExecuteRequest,
)


def fetch_daytona_config() -> DaytonaConfig:
    secret_name = os.environ.get("DAYTONA_SECRET_NAME", "localEvalInfraDaytonaKey")
    client = boto3.client("secretsmanager", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return DaytonaConfig(
        api_key=secret["DAYTONA_API_KEY"],
        api_url=secret["DAYTONA_API_URL"],
        target=secret["DAYTONA_TARGET"],
    )


async def main():
    config = fetch_daytona_config()
    async with AsyncDaytona(config) as daytona:
        sandbox = None
        try:
            print("Creating sandbox with auto_stop_interval=0 (no idle timeout)...")
            sandbox = await daytona.create(
                CreateSandboxFromSnapshotParams(
                    language="python",
                    auto_stop_interval=0,
                ),
                timeout=120,
            )
            print(f"Sandbox created: id={sandbox.id}, state={sandbox.state}")

            session_id = f"test-{uuid.uuid4()}"
            await sandbox.process.create_session(session_id)

            command = "sleep 30 && echo 'done'"
            resp = await sandbox.process.execute_session_command(
                session_id,
                SessionExecuteRequest(command=command, run_async=True),
            )
            cmd_id = resp.cmd_id
            print(f"Started command: {command} (cmd_id={cmd_id})")

            start = time.monotonic()
            command_finished = False

            while True:
                await asyncio.sleep(15)
                elapsed = time.monotonic() - start

                sb = await daytona.get(sandbox.id)

                if not command_finished:
                    cmd = await sandbox.process.get_session_command(session_id, cmd_id)
                    if cmd.exit_code is not None:
                        command_finished = True
                        print(f"[{elapsed:6.1f}s] Command finished (exit_code={cmd.exit_code}). Monitoring sandbox...")
                    else:
                        print(f"[{elapsed:6.1f}s] Command still running, sandbox: {sb.state}")
                        continue

                print(f"[{elapsed:6.1f}s] sandbox state: {sb.state}")

                if sb.state != SandboxState.STARTED:
                    print(f"\nSandbox stopped after {elapsed:.1f}s")
                    print("RESULT: Sandbox was stopped even with auto_stop_interval=0.")
                    return

                if elapsed > 210:
                    print(f"\nSandbox still alive at {elapsed:.1f}s (3 min after creation, ~2.5 min after command finished).")
                    print("RESULT: Sandbox stays alive indefinitely with auto_stop_interval=0.")
                    return

        finally:
            if sandbox:
                print(f"\nCleaning up sandbox {sandbox.id}...")
                try:
                    await daytona.delete(sandbox)
                    print("Sandbox deleted.")
                except Exception as e:
                    print(f"Cleanup error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
