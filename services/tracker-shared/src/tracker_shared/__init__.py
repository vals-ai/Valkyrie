"""Shared types, enums, and exceptions used by the Valkyrie CLI and tracker service.

This lightweight package avoids pulling in heavy backend dependencies
(SQLAlchemy, FastAPI, Daytona, logfire, boto3, etc.) so the CLI starts fast.

Import from submodules directly for best performance::

    from tracker_shared.models import TaskStatus
    from tracker_shared.types import BenchmarkDetails
    from tracker_shared.exceptions import S3Error
"""
