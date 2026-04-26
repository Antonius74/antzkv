"""
AntzKV - Native Python Client Library v2.0

Thread-safe TCP client for antzkv key-value store.
Features:
  - Single-roundtrip CRUD
  - Automatic command pipelining for bulk writes
  - Connection pool
"""

import socket
import threading
import collections
from typing import Optional, List, Tuple

__version__ = "2.0.0"
__all__ = ["AntzKVClient", "AntzKVPool"]


class AntzKVClient:
    """Thread-safe native TCP client for antzkv with pipelining support."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._buf = b""

    def connect(self, timeout: Optional[float] = None) -> None:
        with self._lock:
            if self._sock:
                return
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if timeout is not None:
                self._sock.settimeout(timeout)
            self._sock.connect((self.host, self.port))
            self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self) -> None:
        with self._lock:
            if self._sock:
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self._sock.close()
                self._sock = None
            self._buf = b""

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _send(self, data: str) -> None:
        if self._sock is None:
            raise ConnectionError("Not connected. Call connect() first.")
        self._sock.sendall((data + "\n").encode("utf-8"))

    def _recv_lines(self, n: int) -> List[str]:
        """Read exactly n \n-terminated lines."""
        if self._sock is None:
            raise ConnectionError("Not connected. Call connect() first.")
        lines = []
        while len(lines) < n:
            while b"\n" not in self._buf:
                chunk = self._sock.recv(65536)
                if not chunk:
                    raise ConnectionError("Server closed connection unexpectedly.")
                self._buf += chunk
            idx = self._buf.index(b"\n")
            line = self._buf[:idx].decode("utf-8")
            self._buf = self._buf[idx + 1:]
            if line.endswith("\r"):
                line = line[:-1]
            lines.append(line)
        return lines

    # ---- single op ----

    def _execute(self, cmd: str) -> str:
        with self._lock:
            self._send(cmd)
            return self._recv_lines(1)[0]

    def set(self, key: str, value: str) -> bool:
        return self._execute(f"SET {key} {value}") == "OK"

    def get(self, key: str) -> Optional[str]:
        r = self._execute(f"GET {key}")
        return None if r == "(nil)" else r

    def delete(self, *keys: str) -> int:
        if not keys: return 0
        r = self._execute(f"DEL {' '.join(keys)}")
        try: return int(r)
        except ValueError: return 0

    def exists(self, *keys: str) -> int:
        if not keys: return 0
        r = self._execute(f"EXISTS {' '.join(keys)}")
        try: return int(r)
        except ValueError: return 0

    def keys(self) -> List[str]:
        r = self._execute("KEYS")
        return [] if r == "(empty)" else r.split(" ")

    def save(self) -> bool:
        return self._execute("SAVE") == "OK"

    def ping(self) -> bool:
        return self._execute("PING") == "PONG"

    def quit(self) -> bool:
        return self._execute("QUIT") == "OK"

    # ---- pipeline bulk ops ----

    def pipeline_set(self, pairs: List[Tuple[str, str]]) -> List[bool]:
        """Send multiple SETs in a single network roundtrip."""
        if not pairs: return []
        n = len(pairs)
        with self._lock:
            for k, v in pairs:
                self._send(f"SET {k} {v}")
            replies = self._recv_lines(n)
        return [r == "OK" for r in replies]

    def pipeline_get(self, keys: List[str]) -> List[Optional[str]]:
        """Send multiple GETs in a single network roundtrip."""
        if not keys: return []
        n = len(keys)
        with self._lock:
            for k in keys:
                self._send(f"GET {k}")
            replies = self._recv_lines(n)
        return [None if r == "(nil)" else r for r in replies]

    def pipeline_delete(self, keys: List[str]) -> List[int]:
        if not keys: return []
        n = len(keys)
        with self._lock:
            for k in keys:
                self._send(f"DEL {k}")
            replies = self._recv_lines(n)
        out = []
        for r in replies:
            try: out.append(int(r))
            except ValueError: out.append(0)
        return out


class AntzKVPool:
    """Simple thread-safe connection pool."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379, size: int = 4):
        self._host = host
        self._port = port
        self._size = size
        self._pool: collections.deque = collections.deque()
        self._semaphore = threading.Semaphore(size)
        self._lock = threading.Lock()

    def _create(self) -> AntzKVClient:
        c = AntzKVClient(self._host, self._port)
        c.connect()
        return c

    def acquire(self) -> AntzKVClient:
        self._semaphore.acquire()
        with self._lock:
            if self._pool:
                return self._pool.pop()
        return self._create()

    def release(self, client: AntzKVClient) -> None:
        with self._lock:
            self._pool.append(client)
        self._semaphore.release()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._lock:
            for c in self._pool:
                c.close()
            self._pool.clear()
        return False
