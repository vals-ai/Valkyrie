"""Tests for the early ExecutorHost availability signal."""

from __future__ import annotations

import json
import os
import unittest
from collections.abc import Mapping
from unittest import mock

from botocore.exceptions import ClientError

import executor_rollout_monitor  # pyright: ignore[reportMissingImports]
from executor_rollout_monitor import PrimaryDeployment  # pyright: ignore[reportMissingImports]

_BASELINE = "arn:aws:ecs:us-east-1:123456789012:task-definition/ExecutorHost:1"
_NEW = "arn:aws:ecs:us-east-1:123456789012:task-definition/ExecutorHost:2"
_LOOKUP_ROLE = "arn:aws:iam::123456789012:role/cdk-hnb659fds-lookup-role-123456789012-us-east-1"


def _deployment(
    *,
    task_definition: str = _NEW,
    running_count: int = 1,
    rollout_state: str = "IN_PROGRESS",
    status: str = "PRIMARY",
) -> dict[str, object]:
    return {
        "status": status,
        "taskDefinition": task_definition,
        "runningCount": running_count,
        "rolloutState": rollout_state,
    }


def _client_error(code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": "test error"}},
        "DescribeServices",
    )


def _response(
    *,
    task_definition: str = _NEW,
    running_count: int = 1,
    rollout_state: str = "IN_PROGRESS",
    status: str = "PRIMARY",
) -> dict[str, object]:
    return {
        "services": [
            {
                "deployments": [
                    _deployment(
                        task_definition=task_definition,
                        running_count=running_count,
                        rollout_state=rollout_state,
                        status=status,
                    )
                ]
            }
        ],
        "failures": [],
    }


class ExecutorRolloutMonitorTest(unittest.TestCase):
    def test_assumed_lookup_credentials_create_ecs_client(self) -> None:
        sts = mock.Mock()
        ecs = mock.Mock()
        sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "access",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        with mock.patch.object(
            executor_rollout_monitor.boto3,
            "client",
            side_effect=(sts, ecs),
        ) as client:
            result = executor_rollout_monitor.create_ecs_client(lookup_role_arn=_LOOKUP_ROLE)

        self.assertIs(result, ecs)
        sts.assume_role.assert_called_once_with(
            RoleArn=_LOOKUP_ROLE,
            RoleSessionName="valkyrie-executor-rollout-monitor",
        )
        client.assert_has_calls(
            (
                mock.call("sts"),
                mock.call(
                    "ecs",
                    aws_access_key_id="access",
                    aws_secret_access_key="secret",
                    aws_session_token="token",
                ),
            )
        )

    def test_malformed_assumed_credentials_fail_closed(self) -> None:
        sts = mock.Mock()
        sts.assume_role.return_value = {"Credentials": {"AccessKeyId": "access"}}
        with mock.patch.object(executor_rollout_monitor.boto3, "client", return_value=sts):
            with self.assertRaisesRegex(executor_rollout_monitor.MonitorError, "malformed credentials"):
                executor_rollout_monitor.create_ecs_client(lookup_role_arn=_LOOKUP_ROLE)

    def test_changed_running_primary_is_available_while_in_progress_or_completed(self) -> None:
        for rollout_state in ("IN_PROGRESS", "COMPLETED"):
            with self.subTest(rollout_state=rollout_state):
                deployment = executor_rollout_monitor.primary_deployment(_response(rollout_state=rollout_state))

                self.assertTrue(
                    executor_rollout_monitor.is_new_revision_available(
                        deployment,
                        baseline_task_definition=_BASELINE,
                    )
                )

    def test_baseline_zero_running_and_non_primary_are_not_available(self) -> None:
        cases = (
            _response(task_definition=_BASELINE),
            _response(running_count=0),
            _response(status="ACTIVE"),
        )
        for response in cases:
            with self.subTest(response=response):
                self.assertFalse(
                    executor_rollout_monitor.is_new_revision_available(
                        executor_rollout_monitor.primary_deployment(response),
                        baseline_task_definition=_BASELINE,
                    )
                )

    def test_failed_or_unknown_deployment_is_not_available(self) -> None:
        for rollout_state in ("FAILED", "UNKNOWN"):
            with self.subTest(rollout_state=rollout_state):
                deployment = executor_rollout_monitor.primary_deployment(_response(rollout_state=rollout_state))
                self.assertFalse(
                    executor_rollout_monitor.is_new_revision_available(
                        deployment,
                        baseline_task_definition=_BASELINE,
                    )
                )

    def test_new_primary_alerts_while_old_active_deployment_is_still_running(self) -> None:
        response = {
            "services": [
                {
                    "deployments": [
                        _deployment(),
                        _deployment(
                            task_definition=_BASELINE,
                            running_count=2,
                            rollout_state="COMPLETED",
                            status="ACTIVE",
                        ),
                    ]
                }
            ],
            "failures": [],
        }

        deployment = executor_rollout_monitor.primary_deployment(response)

        self.assertTrue(
            executor_rollout_monitor.is_new_revision_available(
                deployment,
                baseline_task_definition=_BASELINE,
            )
        )

    def test_malformed_or_ambiguous_service_response_fails_closed(self) -> None:
        empty_services: list[object] = []
        service_without_deployments: dict[str, object] = {"deployments": []}
        ambiguous_service: dict[str, object] = {"deployments": [_deployment(), _deployment()]}
        cases: tuple[Mapping[str, object], ...] = (
            {},
            {"services": empty_services},
            {"services": [service_without_deployments]},
            {"services": [ambiguous_service]},
            _response(running_count=-1),
        )
        for response in cases:
            with self.subTest(response=response):
                self.assertIsNone(executor_rollout_monitor.primary_deployment(response))

    def test_monitor_notifies_once_when_new_primary_completes_between_polls(self) -> None:
        responses = iter(
            (
                _response(task_definition=_BASELINE),
                _response(running_count=0),
                _response(rollout_state="COMPLETED"),
            )
        )
        notifications: list[PrimaryDeployment] = []
        now = 0.0

        def monotonic() -> float:
            nonlocal now
            now += 1
            return now

        notified = executor_rollout_monitor.monitor_rollout(
            describe=lambda: next(responses),
            baseline_task_definition=_BASELINE,
            notify=notifications.append,
            timeout_seconds=30,
            poll_seconds=0,
            monotonic=monotonic,
            sleep=lambda _seconds: None,
        )

        self.assertTrue(notified)
        self.assertEqual(notifications, [PrimaryDeployment(_NEW, 1, "COMPLETED")])

    def test_stop_request_triggers_one_final_observation(self) -> None:
        responses = iter((_response(task_definition=_BASELINE), _response(rollout_state="COMPLETED")))
        notifications: list[PrimaryDeployment] = []
        stop_requested = False

        def sleep(_seconds: float) -> None:
            nonlocal stop_requested
            stop_requested = True

        notified = executor_rollout_monitor.monitor_rollout(
            describe=lambda: next(responses),
            baseline_task_definition=_BASELINE,
            notify=notifications.append,
            timeout_seconds=30,
            poll_seconds=5,
            stop_requested=lambda: stop_requested,
            monotonic=lambda: 0,
            sleep=sleep,
        )

        self.assertTrue(notified)
        self.assertEqual(notifications, [PrimaryDeployment(_NEW, 1, "COMPLETED")])

    def test_monitor_times_out_without_notification(self) -> None:
        notifications: list[PrimaryDeployment] = []
        now = 0.0

        def monotonic() -> float:
            return now

        def sleep(seconds: float) -> None:
            nonlocal now
            now += seconds

        notified = executor_rollout_monitor.monitor_rollout(
            describe=lambda: _response(task_definition=_BASELINE),
            baseline_task_definition=_BASELINE,
            notify=notifications.append,
            timeout_seconds=2,
            poll_seconds=1,
            monotonic=monotonic,
            sleep=sleep,
        )

        self.assertFalse(notified)
        self.assertEqual(notifications, [])

    def test_monitor_refreshes_expired_credentials_and_notifies(self) -> None:
        for code in ("ExpiredToken", "ExpiredTokenException"):
            with self.subTest(code=code):
                expired_client = mock.Mock()
                expired_client.describe_services.side_effect = _client_error(code)
                refreshed_client = mock.Mock()
                refreshed_client.describe_services.return_value = _response()
                with (
                    mock.patch.dict(
                        os.environ,
                        {"SLACK_DEPLOY_WEBHOOK_URL": "https://hooks.slack.test"},
                    ),
                    mock.patch.object(
                        executor_rollout_monitor,
                        "create_ecs_client",
                        side_effect=(expired_client, refreshed_client),
                    ) as create_client,
                    mock.patch.object(executor_rollout_monitor, "post_slack") as post_slack,
                ):
                    result = executor_rollout_monitor.main(
                        [
                            "monitor",
                            "--baseline-task-definition",
                            _BASELINE,
                            "--lookup-role-arn",
                            _LOOKUP_ROLE,
                        ]
                    )

                self.assertEqual(result, 0)
                create_client.assert_has_calls(
                    (
                        mock.call(lookup_role_arn=_LOOKUP_ROLE),
                        mock.call(lookup_role_arn=_LOOKUP_ROLE),
                    )
                )
                expired_client.describe_services.assert_called_once()
                refreshed_client.describe_services.assert_called_once()
                post_slack.assert_called_once()

    def test_monitor_does_not_refresh_unrelated_client_error(self) -> None:
        client = mock.Mock()
        client.describe_services.side_effect = _client_error("AccessDeniedException")
        with (
            mock.patch.dict(
                os.environ,
                {"SLACK_DEPLOY_WEBHOOK_URL": "https://hooks.slack.test"},
            ),
            mock.patch.object(executor_rollout_monitor, "create_ecs_client", return_value=client) as create_client,
            mock.patch.object(executor_rollout_monitor, "post_slack") as post_slack,
            mock.patch.object(executor_rollout_monitor, "_warn") as warn,
        ):
            result = executor_rollout_monitor.main(
                [
                    "monitor",
                    "--baseline-task-definition",
                    _BASELINE,
                    "--lookup-role-arn",
                    _LOOKUP_ROLE,
                ]
            )

        self.assertEqual(result, 0)
        create_client.assert_called_once_with(lookup_role_arn=_LOOKUP_ROLE)
        client.describe_services.assert_called_once()
        post_slack.assert_not_called()
        self.assertIn("AccessDeniedException", warn.call_args.args[0])

    def test_monitor_retries_expired_credentials_only_once_per_observation(self) -> None:
        expired_clients = (mock.Mock(), mock.Mock())
        for client in expired_clients:
            client.describe_services.side_effect = _client_error("ExpiredToken")
        with (
            mock.patch.dict(
                os.environ,
                {"SLACK_DEPLOY_WEBHOOK_URL": "https://hooks.slack.test"},
            ),
            mock.patch.object(
                executor_rollout_monitor,
                "create_ecs_client",
                side_effect=expired_clients,
            ) as create_client,
            mock.patch.object(executor_rollout_monitor, "post_slack") as post_slack,
            mock.patch.object(executor_rollout_monitor, "_warn") as warn,
        ):
            result = executor_rollout_monitor.main(
                [
                    "monitor",
                    "--baseline-task-definition",
                    _BASELINE,
                    "--lookup-role-arn",
                    _LOOKUP_ROLE,
                ]
            )

        self.assertEqual(result, 0)
        self.assertEqual(create_client.call_count, 2)
        for client in expired_clients:
            client.describe_services.assert_called_once()
        post_slack.assert_not_called()
        self.assertIn("ExpiredToken", warn.call_args.args[0])

    def test_slack_message_matches_rollout_state(self) -> None:
        cases = (
            (
                "IN_PROGRESS",
                "New ExecutorHost revision has a running task. "
                "Rollout remains in progress; previous tasks may still be draining.",
            ),
            (
                "COMPLETED",
                "New ExecutorHost revision has a running task. Rollout is complete.",
            ),
        )
        for rollout_state, expected_text in cases:
            with self.subTest(rollout_state=rollout_state):
                with mock.patch.object(executor_rollout_monitor.urllib.request, "urlopen") as open_url:
                    executor_rollout_monitor.post_slack(
                        "https://hooks.slack.test",
                        deployment=PrimaryDeployment(_NEW, 1, rollout_state),
                        run_url="https://github.test/actions/runs/123",
                    )

                request = open_url.call_args.args[0]
                payload = json.loads(request.data.decode("utf-8"))
                self.assertEqual(payload["text"], expected_text)
                self.assertNotIn("available", payload["text"])
                self.assertEqual(
                    "previous tasks may still be draining" in payload["text"],
                    rollout_state == "IN_PROGRESS",
                )
                self.assertIn(_NEW, payload["blocks"][1]["elements"][0]["text"])
                self.assertIn(
                    "https://github.test/actions/runs/123",
                    payload["blocks"][1]["elements"][0]["text"],
                )
                open_url.assert_called_once_with(request, timeout=10)

    def test_missing_webhook_and_monitor_error_are_warning_only(self) -> None:
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch.object(executor_rollout_monitor, "create_ecs_client") as create_client,
            mock.patch.object(executor_rollout_monitor, "_warn") as warn,
        ):
            result = executor_rollout_monitor.main(
                [
                    "monitor",
                    "--baseline-task-definition",
                    _BASELINE,
                    "--lookup-role-arn",
                    _LOOKUP_ROLE,
                ]
            )

        self.assertEqual(result, 0)
        create_client.assert_not_called()
        warn.assert_called_once()

        with (
            mock.patch.dict(os.environ, {"SLACK_DEPLOY_WEBHOOK_URL": "https://hooks.slack.test"}),
            mock.patch.object(executor_rollout_monitor, "create_ecs_client"),
            mock.patch.object(
                executor_rollout_monitor,
                "monitor_rollout",
                side_effect=RuntimeError("describe failed"),
            ),
            mock.patch.object(executor_rollout_monitor, "_warn") as warn,
        ):
            result = executor_rollout_monitor.main(
                [
                    "monitor",
                    "--baseline-task-definition",
                    _BASELINE,
                    "--lookup-role-arn",
                    _LOOKUP_ROLE,
                ]
            )

        self.assertEqual(result, 0)
        self.assertIn("without affecting deployment", warn.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
