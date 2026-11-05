#!/usr/bin/env python3
"""Exercise ccapi's real TLS/WebSocket and REST bootstrap against a local server."""
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time


def frame(payload, opcode=2):
    size = len(payload)
    prefix = bytes([0x80 | opcode])
    return prefix + (bytes([size]) if size < 126 else b"\x7e" + struct.pack("!H", size)) + payload


def exact(sock, length):
    data = b""
    while len(data) < length:
        part = sock.recv(length - len(data))
        if not part:
            raise EOFError()
        data += part
    return data


def receive(sock):
    flags, size = exact(sock, 2)
    masked = size & 128
    size &= 127
    if size == 126:
        size = struct.unpack("!H", exact(sock, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", exact(sock, 8))[0]
    mask = exact(sock, 4) if masked else b""
    data = exact(sock, size)
    if mask:
        data = bytes(value ^ mask[i % 4] for i, value in enumerate(data))
    return flags & 15, data


def diff(first, last, qty=250):
    payload = struct.pack("<HHHHqqqbb", 26, 10003, 1, 0, 1720000000000123, first, last, -2, -3)
    payload += struct.pack("<HHqq", 16, 1, 6000010, qty)
    payload += struct.pack("<HHqq", 16, 1, 6000020, 500)
    return payload + b"\x07BTCUSDT"


def main(executable):
    errors = []
    ws_count = 0
    rest_count = 0
    lock = threading.Lock()
    stopping = threading.Event()
    with tempfile.TemporaryDirectory(prefix="ccapi-sbe-") as directory:
        cert, key = Path(directory) / "cert.pem", Path(directory) / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=localhost", "-keyout", str(key), "-out", str(cert)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.2)

        def handle(raw):
            nonlocal ws_count, rest_count
            try:
                with context.wrap_socket(raw, server_side=True) as sock:
                    sock.settimeout(6)
                    request = b""
                    while b"\r\n\r\n" not in request:
                        request += exact(sock, 1)
                    lines = request.decode().split("\r\n")
                    headers = {k.lower(): v.strip() for k, v in (line.split(":", 1) for line in lines[1:] if ":" in line)}
                    if headers.get("upgrade", "").lower() == "websocket":
                        assert headers.get("x-mbx-apikey") == "loopback-key", headers
                        with lock:
                            ws_count += 1
                            number = ws_count
                        accept = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] +
                                  "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
                        sock.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                                      f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())
                        opcode, payload = receive(sock)
                        assert opcode == 1
                        subscription = json.loads(payload)
                        assert subscription["params"] == ["btcusdt@depth"], subscription
                        sock.sendall(frame(json.dumps({"result": None, "id": subscription["id"]}).encode(), 1))
                        initial = 100 if number == 1 else 200
                        sock.sendall(frame(diff(initial + 1, initial + 2)))
                        time.sleep(0.3)
                        sock.sendall(frame(diff(initial + 3, initial + 4, 300)))
                        if number == 1:
                            time.sleep(0.1)
                            sock.sendall(frame(diff(106, 107)))
                        while True:
                            opcode, payload = receive(sock)
                            if opcode == 8:
                                sock.sendall(frame(payload, 8))
                                break
                            if opcode == 9:
                                sock.sendall(frame(payload, 10))
                    else:
                        assert lines[0].startswith("GET /api/v3/depth?symbol=BTCUSDT&limit=5000 "), lines[0]
                        with lock:
                            rest_count += 1
                            number = ws_count
                        body = json.dumps({"lastUpdateId": 100 if number == 1 else 200,
                                           "bids": [["60000.10", "0.20"]], "asks": [["60000.20", "0.40"]]}).encode()
                        sock.sendall((f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n"
                                      "Connection: close\r\n\r\n").encode() + body)
            except (EOFError, ConnectionError):
                if not stopping.is_set():
                    errors.append("unexpected disconnect")
            except Exception as exc:
                errors.append(repr(exc))

        threads = []

        def accept_connections():
            while not stopping.is_set():
                try:
                    raw, _ = listener.accept()
                except socket.timeout:
                    continue
                thread = threading.Thread(target=handle, args=(raw,), daemon=True)
                threads.append(thread)
                thread.start()

        server = threading.Thread(target=accept_connections)
        server.start()
        try:
            env = dict(os.environ, CCAPI_SBE_TEST_PORT=str(listener.getsockname()[1]))
            result = subprocess.run([executable, "--gtest_filter=BinanceSbeLoopback.*"], env=env, timeout=15)
        finally:
            stopping.set()
            server.join()
            listener.close()
            for thread in threads:
                thread.join(timeout=2)
        if errors or ws_count != 2 or rest_count != 2:
            raise RuntimeError(f"Loopback errors={errors}, WebSockets={ws_count}, snapshots={rest_count}")
        return result.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
