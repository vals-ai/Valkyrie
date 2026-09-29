"""Opt-in real TLS/CONNECT tests; no customer credentials or public requests."""

import os
import socket
import ssl
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4

IMAGE = os.getenv("VALSMITH_PROXY_TEST_IMAGE")
SERVICE_HOSTS = (
    "valsmith.vals.ai",
    "prod.benchmarks.vals.ai",
    "re99xljs52.execute-api.us-east-1.amazonaws.com",
    "api.descope.com",
    "app.daytona.io",
    "proxy.app.daytona.io",
)
VIEW_HOST = "model-gateway.vals.ai"


def docker(*arguments: str) -> str:
    result = subprocess.run(["docker", *arguments], text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(f"Docker {arguments[0]} failed: {result.stderr}")
    return (result.stdout + (result.stderr if arguments[0] == "logs" else "")).strip()


def receive_headers(connection: socket.socket) -> bytes:
    response = bytearray()
    while not response.endswith(b"\r\n\r\n"):
        chunk = connection.recv(1)
        if not chunk:
            break

        response.extend(chunk)
        if len(response) > 16384:
            raise AssertionError("Oversized response")

    return bytes(response)


@unittest.skipUnless(IMAGE, "Set VALSMITH_PROXY_TEST_IMAGE to run Docker tunnel qualification")
class ProxyRuntimeTest(unittest.TestCase):
    network: str
    origin: str
    origin_private_address: str
    control_network: str
    proxy: str
    fixture_directory: Path
    ports: dict[int, int]

    @classmethod
    def setUpClass(cls) -> None:
        identifier = uuid4().hex[:10]
        cls.network = f"valsmith-proxy-test-{identifier}"
        cls.control_network = f"{cls.network}-client"
        cls.origin = f"{cls.network}-origin"
        cls.proxy = f"{cls.network}-proxy"
        temporary = tempfile.TemporaryDirectory(prefix="valsmith-proxy-test-")
        cls.addClassCleanup(temporary.cleanup)
        cls.fixture_directory = Path(temporary.name)
        # The public-format address exists only inside an internal Docker network.
        docker("network", "create", "--internal", "--subnet", "11.250.0.0/24", cls.network)
        cls.addClassCleanup(docker, "network", "rm", cls.network)
        docker("network", "create", cls.control_network)
        cls.addClassCleanup(docker, "network", "rm", cls.control_network)
        certificate_config = cls.fixture_directory / "certificate.cnf"
        certificate_config.write_text(
            "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=extensions\n"
            "[dn]\nCN=valsmith.vals.ai\n[extensions]\nsubjectAltName="
            + ",".join(f"DNS:{host}" for host in (*SERVICE_HOSTS, VIEW_HOST))
            + "\n"
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-config",
                str(certificate_config),
                "-keyout",
                str(cls.fixture_directory / "origin.key"),
                "-out",
                str(cls.fixture_directory / "origin.pem"),
            ],
            capture_output=True,
            check=True,
        )
        docker(
            "run",
            "-d",
            "--name",
            cls.origin,
            "--network",
            cls.network,
            "--ip",
            "11.250.0.10",
            "--user",
            "0",
            "--entrypoint",
            "/usr/bin/python3",
            "-v",
            f"{cls.fixture_directory}:/fixtures:ro",
            "-v",
            f"{Path(__file__).with_name('outbound_proxy_runtime.py')}:/origin.py:ro",
            str(IMAGE),
            "/origin.py",
        )
        cls.addClassCleanup(docker, "rm", "-f", cls.origin)
        docker("network", "connect", cls.control_network, cls.origin)
        addresses = docker(
            "inspect", cls.origin, "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}"
        ).split()
        cls.origin_private_address = next(address for address in addresses if address != "11.250.0.10")
        for _ in range(30):
            if docker("exec", cls.origin, "sh", "-c", "test -f /tmp/ready && echo ready || true") == "ready":
                break
            time.sleep(0.1)
        else:
            raise AssertionError("TLS origin failed to start")

        cls.start_proxy()

    @classmethod
    def start_proxy(cls, private_host: str | None = None, private_address: str = "127.0.0.1") -> None:
        arguments = [
            "run",
            "-d",
            "--name",
            cls.proxy,
            "--network",
            cls.control_network,
            "--network",
            cls.network,
            "-p",
            "127.0.0.1::3128",
            "-p",
            "127.0.0.1::3129",
            "--read-only",
            "--tmpfs",
            "/run:uid=13,gid=13",
            "--tmpfs",
            "/tmp:uid=13,gid=13",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
        ]
        for host in (
            *SERVICE_HOSTS,
            VIEW_HOST,
            "unapproved.example",
            "child.valsmith.vals.ai",
            "secret-canary.example",
        ):
            address = private_address if host == private_host else "11.250.0.10"
            arguments.extend(["--add-host", f"{host}:{address}"])
        docker(*arguments, str(IMAGE))
        if private_host is None:
            cls.addClassCleanup(docker, "rm", "-f", cls.proxy)
        cls.ports = {port: int(docker("port", cls.proxy, str(port)).rsplit(":", 1)[1]) for port in (3128, 3129)}
        deadline = time.monotonic() + 10
        probe = cls()
        while time.monotonic() < deadline:
            try:
                if b"origin-success" in probe.request("app.daytona.io"):
                    return
            except (OSError, AssertionError):
                pass
            finally:
                probe.doCleanups()
            time.sleep(0.1)
        raise AssertionError(f"Proxy failed to start: {docker('logs', cls.proxy)}")

    def connect(self, authority: str, port: int = 3128, extra_headers: str = "") -> socket.socket:
        connection = socket.create_connection(("127.0.0.1", self.ports[port]), timeout=4)
        self.addCleanup(connection.close)
        connection.sendall(f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n{extra_headers}\r\n".encode())
        response = receive_headers(connection)
        self.assertIn(b" 200 ", response.split(b"\r\n")[0], response)
        return connection

    def tls(self, connection: socket.socket, server_name: str | None) -> ssl.SSLSocket:
        context = ssl.create_default_context(cafile=str(self.fixture_directory / "origin.pem"))
        context.check_hostname = server_name is not None
        secured = context.wrap_socket(connection, server_hostname=server_name)
        self.addCleanup(secured.close)
        return secured

    def request(self, host: str, port: int = 3128, path: str = "/") -> bytes:
        secured = self.tls(self.connect(f"{host}:443", port), host)
        secured.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
        response = bytearray()
        while chunk := secured.recv(4096):
            response.extend(chunk)
        secured.close()
        return bytes(response)

    def assert_denied(self, authority: str, port: int = 3128) -> None:
        with socket.create_connection(("127.0.0.1", self.ports[port]), timeout=4) as connection:
            connection.sendall(f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n".encode())
            response = receive_headers(connection)
            if b" 200 " in response.split(b"\r\n")[0]:
                # Squid can acknowledge CONNECT before closing a denied TLS tunnel.
                # Disable certificate checks here so origin certificates cannot mask a bypass.
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                host = authority.rsplit(":", 1)[0].split("@")[-1].strip("[]")
                with self.assertRaises((ssl.SSLError, ConnectionError)):
                    with context.wrap_socket(connection, server_hostname=host):
                        pass

    def test_required_hosts_and_distinct_listeners(self) -> None:
        for host in SERVICE_HOSTS:
            with self.subTest(host=host):
                self.assertIn(b"origin-success", self.request(host))
                self.assert_denied(f"{host}:443", 3129)
        self.assertIn(b"origin-success", self.request(VIEW_HOST, 3129))
        self.assert_denied(f"{VIEW_HOST}:443")

    def test_denies_unapproved_authorities_before_tunneling(self) -> None:
        for authority in (
            "unapproved.example:443",
            "child.valsmith.vals.ai:443",
            "11.250.0.10:443",
            "127.0.0.1:443",
            "169.254.169.254:443",
            "169.254.170.2:443",
            "[::1]:443",
            "valsmith.vals.ai:80",
            "valsmith.vals.ai:8443",
            "secret-canary@valsmith.vals.ai:443",
        ):
            with self.subTest(authority=authority):
                self.assert_denied(authority)

    def test_actual_sni_must_match_original_connect_host(self) -> None:
        before = docker("logs", self.origin).count("origin-request")
        for server_name in (None, "app.daytona.io", "model-gateway.vals.ai", "unapproved.example"):
            with self.subTest(server_name=server_name):
                connection = self.connect("valsmith.vals.ai:443")
                with self.assertRaises((ssl.SSLError, ConnectionError)):
                    self.tls(connection, server_name)
        self.assertEqual(docker("logs", self.origin).count("origin-request"), before)

    def test_plaintext_is_not_spliced(self) -> None:
        connection = self.connect("valsmith.vals.ai:443")
        connection.sendall(b"GET /secret-canary HTTP/1.1\r\nHost: valsmith.vals.ai\r\n\r\n")
        try:
            self.assertNotIn(b"origin-success", connection.recv(8192))
        except ConnectionError:
            pass

    def test_tls_stream_arrives_without_truncation(self) -> None:
        response = self.request("valsmith.vals.ai", path="/stream")
        headers, separator, body = response.partition(b"\r\n\r\n")
        self.assertEqual(separator, b"\r\n\r\n")
        self.assertIn(b" 200 ", headers)
        self.assertEqual(body, b"0123456789abcdef" * 16384)

    def test_websocket_remains_bidirectional(self) -> None:
        secured = self.tls(self.connect("proxy.app.daytona.io:443"), "proxy.app.daytona.io")
        secured.sendall(
            b"GET /websocket HTTP/1.1\r\nHost: proxy.app.daytona.io\r\nUpgrade: websocket\r\n"
            b"Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            b"Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.assertIn(b" 101 ", receive_headers(secured))
        self.assertEqual(
            secured.getpeercert(binary_form=True),
            ssl.PEM_cert_to_DER_cert((self.fixture_directory / "origin.pem").read_text()),
        )
        secured.sendall(
            b"\x81\x85\x01\x02\x03\x04" + bytes(byte ^ (1, 2, 3, 4)[index % 4] for index, byte in enumerate(b"hello"))
        )
        self.assertEqual(secured.recv(7), b"\x81\x05hello")

    def test_redirect_cannot_open_an_unapproved_tunnel(self) -> None:
        self.assertIn(b"Location: https://unapproved.example/", self.request("valsmith.vals.ai", path="/redirect"))
        self.assert_denied("unapproved.example:443")

    def test_does_not_log_secret_fields(self) -> None:
        connection = self.connect("valsmith.vals.ai:443", extra_headers="Proxy-Authorization: Bearer secret-canary\r\n")
        secured = self.tls(connection, "valsmith.vals.ai")
        secured.sendall(
            b"GET /secret-canary?token=secret-canary HTTP/1.1\r\nHost: valsmith.vals.ai\r\nAuthorization: Bearer secret-canary\r\n\r\n"
        )
        while secured.recv(4096):
            pass
        secured.close()
        self.assert_denied("secret-canary.example:443")
        time.sleep(0.2)
        logs = docker("logs", self.proxy)
        self.assertIn("valsmith.vals.ai", logs)
        self.assertNotIn("secret-canary", logs)

    def test_y_helper_timeout_does_not_open_a_tunnel(self) -> None:
        helper_processes = docker(
            "exec",
            self.proxy,
            "/usr/bin/python3",
            "-c",
            "from pathlib import Path; "
            "print(' '.join(p.parent.name for p in Path('/proc').glob('[0-9]*/cmdline') "
            "if b'/opt/proxy/sni_acl.py' in p.read_bytes().split(bytes([0]))))",
        ).split()
        self.assertTrue(helper_processes)
        before = docker("logs", self.origin).count("origin-request")
        try:
            docker(
                "exec",
                self.proxy,
                "/usr/bin/python3",
                "-c",
                "import os, signal, sys; [os.kill(int(pid), signal.SIGSTOP) for pid in sys.argv[1:]]",
                *helper_processes,
            )
            connection = self.connect("valsmith.vals.ai:443")
            context = ssl.create_default_context(cafile=str(self.fixture_directory / "origin.pem"))
            incoming = ssl.MemoryBIO()
            outgoing = ssl.MemoryBIO()
            client = context.wrap_bio(incoming, outgoing, server_hostname="valsmith.vals.ai")
            with self.assertRaises(ssl.SSLWantReadError):
                client.do_handshake()
            connection.sendall(outgoing.read())
            connection.settimeout(40)
            self.assertEqual(connection.recv(1), b"", "Proxy must close while the helper is stopped")
            self.assertEqual(docker("logs", self.origin).count("origin-request"), before)
        finally:
            docker(
                "exec",
                self.proxy,
                "/usr/bin/python3",
                "-c",
                "import os, signal, sys; [os.kill(int(pid), signal.SIGCONT) for pid in sys.argv[1:]]",
                *helper_processes,
            )
        self.assertEqual(connection.recv(1), b"", "Resuming the helper must not reopen the old tunnel")
        self.assertIn(b"origin-success", self.request("valsmith.vals.ai"))

    def test_z_private_dns_is_denied(self) -> None:
        # Only DNS changes. The image keeps its fixed production policy.
        for address in (
            self.origin_private_address,
            "127.0.0.1",
            "10.0.0.10",
            "169.254.169.254",
            "169.254.170.2",
            "::1",
        ):
            with self.subTest(address=address):
                docker("rm", "-f", self.proxy)
                self.start_proxy(private_host="valsmith.vals.ai", private_address=address)
                self.assert_denied("valsmith.vals.ai:443")
                self.assertIn(b"origin-success", self.request("app.daytona.io"))
