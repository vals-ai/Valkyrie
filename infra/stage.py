"""Stage-aware naming helper.

prod  → every helper is the identity function, so `cdk synth -c stage=prod` is
        byte-identical to the pre-refactor `cdk synth`.
dev   → stacks get a "Dev-" construct-id prefix; physical names get a "-dev"
        suffix; FQDNs get "-dev" on their first label.

"""

from __future__ import annotations

import aws_cdk as cdk

PROD = "prod"
DEV = "dev"


class Stage:
    def __init__(self, name: str) -> None:
        self.name: str = name

    @property
    def is_prod(self) -> bool:
        return self.name == PROD

    def stack_id(self, base: str) -> str:
        return base if self.is_prod else f"Dev-{base}"

    def phys(self, name: str) -> str:
        return name if self.is_prod else f"{name}-dev"

    def domain(self, fqdn: str) -> str:
        # "benchmark-tracker.vals.ai" -> "benchmark-tracker-dev.vals.ai"
        assert "." in fqdn, f"domain() requires a FQDN with a dot, got {fqdn!r}"
        if self.is_prod:
            return fqdn
        first, _, rest = fqdn.partition(".")
        return f"{first}-dev.{rest}"


def resolve(app: cdk.App) -> Stage:
    name = app.node.try_get_context("stage") or PROD
    if name not in (PROD, DEV):
        raise ValueError(f"unknown stage {name!r}; expected {PROD!r} or {DEV!r}")
    return Stage(name)
