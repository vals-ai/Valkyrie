"""Controlled TLS origin used only by the Docker proxy qualification tests."""

import base64
import hashlib
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Origin(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass

    def do_GET(self) -> None:
        print("origin-request", flush=True)
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


def main() -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain("/fixtures/origin.pem", "/fixtures/origin.key")
    server = ThreadingHTTPServer(("0.0.0.0", 443), Origin)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    Path("/tmp/ready").touch()
    server.serve_forever()


if __name__ == "__main__":
    main()
