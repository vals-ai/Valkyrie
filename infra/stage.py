"""Stage-aware naming helper.

prod  -> every helper is the identity function, so `cdk synth -c stage=prod` is
        byte-identical to the pre-refactor `cdk synth`.
prod-external -> production-grade deployment that lives beside prod in the same
        AWS account: stacks get a "ValkProdExternal" construct-id prefix,
        physical names get a "-prod-external" suffix, and the Tracker FQDN gets
        "-external" on its first label.
dev   -> stacks get a "ValkDev" construct-id prefix; physical names get a "-dev"
        suffix; FQDNs get "-dev" on their first label.
release-test -> stacks get a "ValkReleaseTest" prefix and "-release-test"
        physical/FQDN suffixes, while remaining in the dev AWS account.

"""

from __future__ import annotations

import aws_cdk as cdk

PROD = "prod"
PROD_EXTERNAL = "prod-external"
DEV = "dev"
RELEASE_TEST = "release-test"
DEV_STACK_PREFIX = "ValkDev"
PROD_EXTERNAL_STACK_PREFIX = "ValkProdExternal"
RELEASE_TEST_STACK_PREFIX = "ValkReleaseTest"
_SUPPORTED_STAGES = (PROD, PROD_EXTERNAL, DEV, RELEASE_TEST)

# prod owns the unsuffixed names; every other stage carries its own identity.
_STACK_PREFIXES = {
    PROD: "",
    PROD_EXTERNAL: PROD_EXTERNAL_STACK_PREFIX,
    DEV: DEV_STACK_PREFIX,
    RELEASE_TEST: RELEASE_TEST_STACK_PREFIX,
}
_DOMAIN_LABELS = {PROD_EXTERNAL: "external", DEV: DEV, RELEASE_TEST: RELEASE_TEST}


class Stage:
    def __init__(self, name: str) -> None:
        self.name: str = name

    @property
    def is_production(self) -> bool:
        """Whether the stage serves production traffic and takes production settings."""
        return self.name in (PROD, PROD_EXTERNAL)

    @property
    def is_primary_prod(self) -> bool:
        """Whether the stage owns the original unsuffixed physical names."""
        return self.name == PROD

    @property
    def is_release_test(self) -> bool:
        return self.name == RELEASE_TEST

    def stack_id(self, base: str) -> str:
        return f"{_STACK_PREFIXES[self.name]}{base}"

    def phys(self, name: str) -> str:
        return name if self.is_primary_prod else f"{name}-{self.name}"

    def domain(self, fqdn: str) -> str:
        # "benchmark-tracker.vals.ai" -> "benchmark-tracker-dev.vals.ai"
        assert "." in fqdn, f"domain() requires a FQDN with a dot, got {fqdn!r}"
        if self.is_primary_prod:
            return fqdn
        first, _, rest = fqdn.partition(".")
        return f"{first}-{_DOMAIN_LABELS[self.name]}.{rest}"


def resolve(app: cdk.App) -> Stage:
    name = app.node.try_get_context("stage") or PROD
    if name not in _SUPPORTED_STAGES:
        raise ValueError(f"unknown stage {name!r}; expected one of {_SUPPORTED_STAGES!r}")
    return Stage(name)
