"""Regenerate the private operator contract documentation from typed models."""

import json
from pathlib import Path

from tracker.run_transfer.contracts import TransferRequest, TransferResponse


def main() -> None:
    destination = Path(__file__).resolve().parents[3] / "docs" / "contracts"
    for name, model in (("request", TransferRequest), ("response", TransferResponse), ("inspect", TransferResponse)):
        schema = model.model_json_schema()
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        if name == "inspect":
            schema["properties"]["action"] = {"const": "inspect", "type": "string"}
        (destination / f"tracker-transfer-{name}-v1.schema.json").write_text(json.dumps(schema, indent=2) + "\n")


if __name__ == "__main__":
    main()
