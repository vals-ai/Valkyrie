"""Sealed dispatch input integrity and provider selection."""

import base64
from dataclasses import replace
from uuid import uuid4

import pytest
from cryptography.exceptions import InvalidTag

import tracker.executor.dispatch_payload as dispatch_payload
from tracker.executor.dispatch_payload import open_payload, seal_payload


@pytest.fixture
def local_payload_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    monkeypatch.delenv("EXECUTOR_PAYLOAD_KMS_KEY_ID", raising=False)


def test_sealed_payload_roundtrip_without_plaintext(local_payload_key: None) -> None:
    dispatch_id = uuid4()
    payload = {"secret": "unique-sensitive-marker", "telemetry_context_json": {"request_id": "request-1"}}
    sealed = seal_payload(dispatch_id, payload)

    assert open_payload(dispatch_id, sealed) == payload
    assert b"unique-sensitive-marker" not in sealed.ciphertext
    assert b"unique-sensitive-marker" not in sealed.encrypted_data_key


def test_sealed_payload_rejects_another_dispatch(local_payload_key: None) -> None:
    sealed = seal_payload(uuid4(), {"secret": "sensitive"})
    with pytest.raises(InvalidTag):
        open_payload(uuid4(), sealed)


def test_sealed_payload_rejects_ciphertext_tampering(local_payload_key: None) -> None:
    dispatch_id = uuid4()
    sealed = seal_payload(dispatch_id, {"secret": "sensitive"})
    altered = replace(sealed, ciphertext=sealed.ciphertext[:-1] + bytes([sealed.ciphertext[-1] ^ 1]))
    with pytest.raises(InvalidTag):
        open_payload(dispatch_id, altered)


def test_local_payload_key_refused_for_ecs_launcher(local_payload_key: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    with pytest.raises(ValueError, match="Local payload key requires"):
        seal_payload(uuid4(), {})


def test_kms_payload_context_and_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    class KmsClient:
        def __init__(self) -> None:
            self.generated: list[dict[str, object]] = []
            self.decrypted: list[dict[str, object]] = []
            self.data_key = b"d" * 32
            self.blob = b"encrypted-data-key"

        def generate_data_key(self, **kwargs: object) -> dict[str, bytes]:
            self.generated.append(kwargs)
            return {"Plaintext": self.data_key, "CiphertextBlob": self.blob}

        def decrypt(self, **kwargs: object) -> dict[str, bytes]:
            self.decrypted.append(kwargs)
            if (
                kwargs["CiphertextBlob"] != self.blob
                or kwargs["EncryptionContext"] != self.generated[-1]["EncryptionContext"]
            ):
                raise ValueError("Unrecognized data key or encryption context")
            return {"Plaintext": self.data_key}

    kms = KmsClient()
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "ecs")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_KMS_KEY_ID", "arn:aws:kms:us-east-1:123456789012:key/test")
    monkeypatch.delenv("EXECUTOR_PAYLOAD_LOCAL_KEY", raising=False)
    monkeypatch.setattr(
        dispatch_payload.boto3, "client", lambda service: kms if service == "kms" else pytest.fail(service)
    )
    dispatch_id = uuid4()
    payload = {"service_headers": {"authorization": "unique-kms-sensitive-marker"}}
    sealed = seal_payload(dispatch_id, payload)
    assert open_payload(dispatch_id, sealed) == payload
    assert kms.generated == [
        {
            "KeyId": "arn:aws:kms:us-east-1:123456789012:key/test",
            "KeySpec": "AES_256",
            "EncryptionContext": {"dispatch_id": str(dispatch_id)},
        }
    ]
    assert kms.decrypted == [
        {
            "CiphertextBlob": sealed.encrypted_data_key,
            "EncryptionContext": {"dispatch_id": str(dispatch_id)},
        }
    ]
    with pytest.raises(ValueError, match="encryption context"):
        open_payload(uuid4(), sealed)
    assert kms.decrypted[-1]["EncryptionContext"] != kms.generated[0]["EncryptionContext"]
    altered = replace(sealed, ciphertext=sealed.ciphertext[:-1] + bytes([sealed.ciphertext[-1] ^ 1]))
    with pytest.raises(InvalidTag):
        open_payload(dispatch_id, altered)
