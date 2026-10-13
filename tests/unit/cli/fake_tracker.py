import json

import httpx


class FakeTracker:
    """Serve the Tracker's provider and run-settings routes from memory."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.refusal: httpx.Response | None = None
        self.providers: dict[str, str] = {}
        self.default_provider: str | None = None
        self.benchmark_services: dict[str, dict[str, str | None]] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        body = json.loads(request.content) if request.content else {}
        if self.refusal is not None:
            return self.refusal
        if path.startswith("/sandbox-providers/"):
            name = path.removeprefix("/sandbox-providers/")
            if request.method == "PUT":
                self.providers[name] = body["secret_name"]
            elif self.providers.pop(name, None) is None:
                return httpx.Response(404, json={"detail": f"Provider '{name}' has no secret"})
        elif path == "/settings/default-provider":
            self.default_provider = body["provider"]
        elif path.startswith("/settings/benchmark-services/"):
            name = path.removeprefix("/settings/benchmark-services/")
            if request.method == "PUT":
                self.benchmark_services[name] = {"auth_header_name": None, "auth_secret_name": None, **body}
            elif self.benchmark_services.pop(name, None) is None:
                return httpx.Response(404, json={"detail": f"Benchmark '{name}' has no saved URL"})
        elif path not in ("/sandbox-providers", "/settings"):
            return httpx.Response(404, json={"detail": "Not Found"})
        if path.startswith("/sandbox-providers"):
            return httpx.Response(200, json={"providers": self.providers})
        settings = {"default_provider": self.default_provider, "benchmark_services": self.benchmark_services}
        return httpx.Response(200, json=settings)
