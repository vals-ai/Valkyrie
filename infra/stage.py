"""Stage-aware naming helper.

prod  -> every helper is the identity function, so `cdk synth -c stage=prod` is
        byte-identical to the pre-refactor `cdk synth`.
dev   -> stacks get a "ValkDev" construct-id prefix; physical names get a "-dev"
        suffix; FQDNs get "-dev" on their first label.
release-test -> stacks get a "ValkReleaseTest" prefix and "-release-test"
        physical/FQDN suffixes, while remaining in the dev AWS account.

"""

from __future__ import annotations

import aws_cdk as cdk

PROD = "prod"
DEV = "dev"
RELEASE_TEST = "release-test"
DEV_STACK_PREFIX = "ValkDev"
RELEASE_TEST_STACK_PREFIX = "ValkReleaseTest"
_SUPPORTED_STAGES = (PROD, DEV, RELEASE_TEST)


class Stage:
    def __init__(self, name: str) -> None:
        self.name: str = name

    @property
    def is_prod(self) -> bool:
        return self.name == PROD

    @property
    def is_release_test(self) -> bool:
        return self.name == RELEASE_TEST

    def stack_id(self, base: str) -> str:
        if self.is_prod:
            return base
        prefix = RELEASE_TEST_STACK_PREFIX if self.is_release_test else DEV_STACK_PREFIX
        return f"{prefix}{base}"

    def phys(self, name: str) -> str:
        return name if self.is_prod else f"{name}-{self.name}"

    def domain(self, fqdn: str) -> str:
        # "benchmark-tracker.vals.ai" -> "benchmark-tracker-dev.vals.ai"
        assert "." in fqdn, f"domain() requires a FQDN with a dot, got {fqdn!r}"
        if self.is_prod:
            return fqdn
        first, _, rest = fqdn.partition(".")
        return f"{first}-{self.name}.{rest}"


def resolve(app: cdk.App) -> Stage:
    name = app.node.try_get_context("stage") or PROD
    if name not in _SUPPORTED_STAGES:
        raise ValueError(f"unknown stage {name!r}; expected one of {_SUPPORTED_STAGES!r}")
    return Stage(name)
