"""Short-lived credentials for the managed model gateway."""

import time
from urllib.parse import urlsplit
from uuid import UUID

import jwt

from tracker import config
from tracker.exceptions import TrackerServiceError

_TOKEN_TTL_SECONDS = 24 * 60 * 60


def model_gateway_origin() -> str:
    try:
        url = urlsplit(config.MODEL_GATEWAY_URL)
        _ = url.port
    except ValueError:
        raise TrackerServiceError("Managed model gateway URL is invalid") from None
    if (
        url.scheme not in {"http", "https"}
        or not url.netloc
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise TrackerServiceError("Managed model gateway URL is invalid")
    return f"{url.scheme}://{url.netloc}"


def sign_model_gateway_token(access_key_id: str, org_id: UUID, run_id: UUID, task_id: str) -> str:
    issued_at = int(time.time())
    return jwt.encode(
        {
            "sub": access_key_id,
            "org_id": str(org_id),
            "run_id": str(run_id),
            "task_id": task_id,
            "iat": issued_at,
            "exp": issued_at + _TOKEN_TTL_SECONDS,
            "iss": "valkyrie-tracker",
            "aud": "model-gateway",
        },
        config.VALKYRIE_GATEWAY_SIGNING_KEY,
        algorithm="HS256",
    )
