"""Write a shields.io endpoint badge from a Cobertura coverage.xml report."""

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path


def badge_color(percent: float) -> str:
    """Return the shields.io color for a coverage percentage."""
    if percent >= 90:
        return "brightgreen"
    if percent >= 80:
        return "green"
    if percent >= 70:
        return "yellow"
    return "red"


def main() -> None:
    """Read the report's line rate and write the endpoint JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("label")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()

    percent = float(ET.parse(args.report).getroot().attrib["line-rate"]) * 100
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "label": args.label,
                "message": f"{percent:.0f}%",
                "color": badge_color(percent),
            }
        )
    )


if __name__ == "__main__":
    main()
