"""Stage-aware naming helper.

bench -> deploys to the benchmark account while retaining the established prod
        stack ids, physical names, parameter paths, and Tracker FQDN.
prod  -> externally facing production account: stacks get a "ValkProd"
        construct-id prefix and physical/FQDN names get a "-prod" suffix.
dev   -> stacks get a "ValkDev" construct-id prefix; physical names get a "-dev"
        suffix; FQDNs get "-dev" on their first label.
release-test -> stacks get a "ValkReleaseTest" prefix and "-release-test"
        physical/FQDN suffixes, while remaining in the dev AWS account.

"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import aws_cdk as cdk

BENCH = "bench"
PROD = "prod"
DEV = "dev"
RELEASE_TEST = "release-test"
DEV_STACK_PREFIX = "ValkDev"
PROD_STACK_PREFIX = "ValkProd"
RELEASE_TEST_STACK_PREFIX = "ValkReleaseTest"
_SUPPORTED_STAGES = (BENCH, PROD, DEV, RELEASE_TEST)

# Bench retains the resource identity deployed before the account was renamed.
_STACK_PREFIXES = {
    BENCH: "",
    PROD: PROD_STACK_PREFIX,
    DEV: DEV_STACK_PREFIX,
    RELEASE_TEST: RELEASE_TEST_STACK_PREFIX,
}
_RESOURCE_STAGE_NAMES = {BENCH: "prod", PROD: PROD, DEV: DEV, RELEASE_TEST: RELEASE_TEST}
_GITHUB_ENVIRONMENTS = {BENCH: "prod", PROD: "prod-external", DEV: DEV, RELEASE_TEST: RELEASE_TEST}


def resource_stage_name(stage_name: str) -> str:
    """Return the stable stage component used by deployed resource contracts."""
    return _RESOURCE_STAGE_NAMES[stage_name]


class Stage:
    def __init__(self, name: str) -> None:
        self.name: str = name

    @property
    def is_production(self) -> bool:
        """Whether the stage serves production traffic and takes production settings."""
        return self.name in (BENCH, PROD)

    @property
    def is_bench(self) -> bool:
        """Whether the stage owns the established unsuffixed resource names."""
        return self.name == BENCH

    @property
    def is_release_test(self) -> bool:
        return self.name == RELEASE_TEST

    @property
    def github_environment(self) -> str:
        """Return the stable GitHub OIDC environment subject for this stage."""
        return _GITHUB_ENVIRONMENTS[self.name]

    def stack_id(self, base: str) -> str:
        return f"{_STACK_PREFIXES[self.name]}{base}"

    def phys(self, name: str) -> str:
        return name if self.is_bench else f"{name}-{self.name}"

    def domain(self, fqdn: str) -> str:
        # "benchmark-tracker.vals.ai" -> "benchmark-tracker-dev.vals.ai"
        assert "." in fqdn, f"domain() requires a FQDN with a dot, got {fqdn!r}"
        if self.is_bench:
            return fqdn
        first, _, rest = fqdn.partition(".")
        return f"{first}-{self.name}.{rest}"


def resolve(app: cdk.App) -> Stage:
    name = app.node.try_get_context("stage") or BENCH
    if name not in _SUPPORTED_STAGES:
        raise ValueError(f"unknown stage {name!r}; expected one of {_SUPPORTED_STAGES!r}")
    return Stage(name)
