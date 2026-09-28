"""Envelope-encrypted execution inputs for a single executor dispatch."""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import boto3
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass(frozen=True)
class SealedPayload:
    ciphertext: bytes
    encrypted_data_key: bytes
    nonce: bytes


def _key_configuration() -> tuple[str, bytes | str]:
    launcher = os.environ["EXECUTOR_LAUNCHER"]
    if launcher not in ("ecs", "local"):
        raise ValueError("EXECUTOR_LAUNCHER must be ecs or local")
    local_key = os.environ.get("EXECUTOR_PAYLOAD_LOCAL_KEY")
    kms_key_id = os.environ.get("EXECUTOR_PAYLOAD_KMS_KEY_ID")
    if local_key is not None:
        if launcher != "local" or kms_key_id is not None:
            raise ValueError("Local payload key requires the local launcher and no KMS key")
        key = base64.b64decode(local_key, validate=True)
        if len(key) != 32:
            raise ValueError("EXECUTOR_PAYLOAD_LOCAL_KEY must decode to 32 bytes")
        return "local", key
    if kms_key_id is None:
        raise ValueError("Executor payload encryption key is required")
    return "kms", kms_key_id


def seal_payload(dispatch_id: UUID, payload: dict[str, Any]) -> SealedPayload:
    provider, key = _key_configuration()
    aad = str(dispatch_id).encode()
    if provider == "kms":
        response = boto3.client("kms").generate_data_key(
            KeyId=key, KeySpec="AES_256", EncryptionContext={"dispatch_id": str(dispatch_id)}
        )
        data_key = response["Plaintext"]
        encrypted_data_key = response["CiphertextBlob"]
    else:
        data_key = os.urandom(32)
        wrap_nonce = os.urandom(12)
        encrypted_data_key = wrap_nonce + AESGCM(key).encrypt(wrap_nonce, data_key, aad)
    nonce = os.urandom(12)
    ciphertext = AESGCM(data_key).encrypt(nonce, json.dumps(payload).encode(), aad)
    return SealedPayload(ciphertext=ciphertext, encrypted_data_key=encrypted_data_key, nonce=nonce)


def open_payload(dispatch_id: UUID, sealed: SealedPayload) -> dict[str, Any]:
    provider, key = _key_configuration()
    aad = str(dispatch_id).encode()
    if provider == "kms":
        data_key = boto3.client("kms").decrypt(
            CiphertextBlob=sealed.encrypted_data_key,
            EncryptionContext={"dispatch_id": str(dispatch_id)},
        )["Plaintext"]
    else:
        data_key = AESGCM(key).decrypt(
            sealed.encrypted_data_key[:12], sealed.encrypted_data_key[12:], aad
        )
    return json.loads(AESGCM(data_key).decrypt(sealed.nonce, sealed.ciphertext, aad))
