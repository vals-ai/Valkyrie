"""Controlled TLS origin used only by the Docker proxy qualification tests."""

import base64
import hashlib
import socket
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Origin(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        print(f"origin-request {self.path}", flush=True)
        if self.headers.get("Upgrade", "").lower() == "websocket":
            key = self.headers["Sec-WebSocket-Key"]
            accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept.decode())
            self.end_headers()
            frame = self.rfile.read(2)
            length = frame[1] & 127
            mask = self.rfile.read(4)
            masked = self.rfile.read(length)
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(masked))
            self.wfile.write(bytes([0x81, len(payload)]) + payload)
            self.wfile.flush()
            return

        if self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Length", "262144")
            self.end_headers()
            for _ in range(64):
                self.wfile.write(b"0123456789abcdef" * 256)
                self.wfile.flush()
            return

        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "https://unapproved.example/secret-canary")
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Length", "14")
        self.end_headers()
        self.wfile.write(b"origin-success")


class ConcurrentTlsServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, address: tuple[str, int], context: ssl.SSLContext) -> None:
        self.context = context
        super().__init__(address, Origin)

    def process_request_thread(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: tuple[str, int]
    ) -> None:
        if not isinstance(request, socket.socket):
            raise TypeError("TLS origin requires a TCP socket")

        try:
            request.settimeout(10)
            with self.context.wrap_socket(request, server_side=True) as secured:
                super().process_request_thread(secured, client_address)
        except (ssl.SSLError, OSError):
            request.close()


def main() -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("/fixtures/origin.pem", "/fixtures/origin.key")
    server = ConcurrentTlsServer(("0.0.0.0", 443), context)
    Path("/tmp/ready").touch()
    server.serve_forever()


if __name__ == "__main__":
    main()
