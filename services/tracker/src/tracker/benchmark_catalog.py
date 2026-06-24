"""Hosted benchmark catalog used for access listing."""

import json
import os
from typing import cast

HOSTED_BENCHMARK_DATASETS: dict[str, tuple[str, ...]] = {
    "code-migration": ("cobol", "cli", "smoke", "public", "test", "validation"),
    "cyberbench": ("default", "poc_l0", "patch", "poc_patch"),
    "deep-swe": ("default", "deep-swe"),
    "deepmind-formal-conjectures": (
        "default",
        "fc100_open",
        "fc100_solved",
        "open",
        "non_open",
        "research_solved_known_formal",
        "all",
    ),
    "emb": ("default",),
    "fabv2-anthropic": ("validation",),
    "fabv2-exa": ("validation",),
    "fabv2-internal": ("default", "validation", "test", "public", "vals_index"),
    "fabv2-meta": ("validation",),
    "fabv2-xai": ("validation",),
    "harvey-legal-agent": ("default",),
    "ioi": ("ioi2024", "ioi2025"),
    "legal-research": ("default", "testing", "full", "test", "validation", "public"),
    "programbench": ("default", "smoke"),
    "proof-bench": ("validation", "test", "default"),
    "skillsbench": ("default",),
    "snap": (
        "sample_1_target_both",
        "sample_1_target_multi_turn",
        "sample_1_target_web_search",
        "sample_1_target_neither",
        "sample_1_auditor_both",
        "sample_1_auditor_multi_turn",
        "sample_1_auditor_web_search",
        "sample_1_auditor_neither",
        "sample_2_target_both",
        "sample_2_target_multi_turn",
        "sample_2_target_web_search",
        "sample_2_target_neither",
        "sample_2_auditor_both",
        "sample_2_auditor_multi_turn",
        "sample_2_auditor_web_search",
        "sample_2_auditor_neither",
    ),
    "swebench": ("default", "vals_index"),
    "terminal-bench": ("default", "terminal-bench-2", "terminal-bench-2.1"),
    "vcb": ("default", "test_set", "validation_set", "vals_index", "zeeter"),
    "vcb-1-100": ("default", "candidate", "smoke"),
}


def _normalize_catalog(raw: object) -> dict[str, tuple[str, ...]]:
    if not isinstance(raw, dict):
        raise ValueError("benchmark catalog must be a JSON object")

    catalog: dict[str, tuple[str, ...]] = {}
    raw_catalog = cast(dict[object, object], raw)
    for benchmark_name, datasets in raw_catalog.items():
        if not isinstance(benchmark_name, str):
            raise ValueError("benchmark catalog names must be strings")
        if not isinstance(datasets, list):
            raise ValueError(f"benchmark catalog datasets for {benchmark_name!r} must be a list of strings")
        dataset_values = cast(list[object], datasets)
        if not all(isinstance(dataset, str) for dataset in dataset_values):
            raise ValueError(f"benchmark catalog datasets for {benchmark_name!r} must be a list of strings")
        catalog[benchmark_name] = tuple(dataset for dataset in dataset_values if isinstance(dataset, str))
    return dict(sorted(catalog.items()))


def hosted_benchmark_catalog() -> dict[str, tuple[str, ...]]:
    catalog_json = os.environ.get("BENCHMARK_DATASET_CATALOG_JSON")
    if catalog_json:
        return _normalize_catalog(json.loads(catalog_json))
    return dict(sorted(HOSTED_BENCHMARK_DATASETS.items()))
