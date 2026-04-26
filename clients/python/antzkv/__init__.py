"""
AntzKV - Native Python Client Library

Thread-safe TCP client for antzkv key-value store.
No CLI subprocesses - pure socket communication.

Usage:
    from antzkv import AntzKVClient

    client = AntzKVClient('127.0.0.1', 6379)
    client.connect()
    client.set('name', 'Alice')
    print(client.get('name'))  # Alice
    client.close()
"""

import socket
import threading
from typing import Optional, List

__version__ = "1.0.0"
__all__ = ["AntzKVClient", "AntzKVPool"]


class AntzKVClient:
    """Thread-safe native TCP client for antzkv."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._buf = b""

    def connect(self, timeout: Optional[float] = None) -> None:
        """Open TCP connection to the server."""
        with self._lock:
            if self._sock:
                return
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if timeout is not None:
                self._sock.settimeout(timeout)
            self._sock.connect((self.host, self.port))

    def close(self) -> None:
        """Close the connection."""
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
        """Send a line (thread-safe)."""
        if self._sock is None:
            raise ConnectionError("Not connected. Call connect() first.")
        self._sock.sendall((data + "\n").encode("utf-8"))

    def _recv_line(self) -> str:
        """Read one \\n-terminated line (thread-safe)."""
        if self._sock is None:
            raise ConnectionError("Not connected. Call connect() first.")
        while b"\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("Server closed connection unexpectedly.")
            self._buf += chunk
        idx = self._buf.index(b"\n")
        line = self._buf[:idx].decode("utf-8")
        self._buf = self._buf[idx + 1:]
        # strip \\r if present
        if line.endswith("\r"):
            line = line[:-1]
        return line

    def _execute(self, cmd: str) -> str:
        """Send command and return reply (atomic under lock)."""
        with self._lock:
            self._send(cmd)
            return self._recv_line()

    # ---- CRUD ----

    def set(self, key: str, value: str) -> bool:
        """Store value under key. Returns True on success."""
        return self._execute(f"SET {key} {value}") == "OK"

    def get(self, key: str) -> Optional[str]:
        """Retrieve value. Returns None if key does not exist."""
        r = self._execute(f"GET {key}")
        return None if r == "(nil)" else r

    def delete(self, *keys: str) -> int:
        """Delete one or more keys. Returns number of keys removed."""
        if not keys:
            return 0
        r = self._execute(f"DEL {' '.join(keys)}")
        try:
            return int(r)
        except ValueError:
            return 0

    def exists(self, *keys: str) -> int:
        """Check existence. Returns count of existing keys."""
        if not keys:
            return 0
        r = self._execute(f"EXISTS {' '.join(keys)}")
        try:
            return int(r)
        except ValueError:
            return 0

    def keys(self) -> List[str]:
        """Return all keys. Returns empty list if none."""
        r = self._execute("KEYS")
        if r == "(empty)":
            return []
        return r.split(" ")

    def save(self) -> bool:
        """Explicit snapshot to disk. Returns True on success."""
        return self._execute("SAVE") == "OK"

    def ping(self) -> bool:
        """Health check. Returns True if server responds PONG."""
        return self._execute("PING") == "PONG"

    def quit(self) -> bool:
        """Gracefully close server-side connection."""
        return self._execute("QUIT") == "OK"


class AntzKVPool:
    """Simple thread-safe connection pool."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6379, size: int = 4):
        self._host = host
        self._port = port
        self._size = size
        self._pool: List[AntzKVClient] = []
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
