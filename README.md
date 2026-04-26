# antzkv

`antzkv` is a lightweight, in-memory / file-persisted **key-value database** inspired by Redis, written in portable **C11** with POSIX threads. It supports concurrent client access over TCP, a simple text-line protocol, and both interactive and non-interactive CLI modes.

**New in cluster branch:** multi-node replication with automatic mesh networking, Last-Write-Wins conflict resolution, and configurable per-node persistence.

---

## Table of Contents

1. [Technical Overview](#1-technical-overview)
2. [Architecture](#2-architecture)
3. [Cluster Mode](#3-cluster-mode)
   1. [Configuration](#31-configuration)
   2. [Replication Model](#32-replication-model)
4. [API / Wire Protocol](#4-api--wire-protocol)
5. [Build](#5-build)
6. [User Manual](#6-user-manual)
   1. [Server Options](#61-server-options)
   2. [CLI Options](#62-cli-options)
   3. [Command Reference](#63-command-reference)
7. [Testing](#7-testing)
8. [Client Libraries](#8-client-libraries)
9. [Performance](#9-performance)
10. [License](#10-license)

---

## 1. Technical Overview

* **Language**: C11 (POSIX.1-2008)
* **Concurrency**: read-write lock (`pthread_rwlock_t`) on the internal hash table + one `pthread` per accepted client.
* **Storage model**: open-addressing hash table with quadratic probing.
* **Persistence**: optional append-rewrite style binary file (custom format).
* **Protocol**: plain TCP, newline-terminated text commands and replies.
* **Default port**: `6379`
* **Clustering** (new): mesh TCP overlay, heartbeat-based failure detection, async replication.

---

## 2. Architecture

### 2.1 Directory Layout

```
.
├── include/
│   └── kvdb.h          # Public C API
├── src/
│   ├── core/
│   │   └── kvdb.c      # Hash table + persistence engine (with metadata)
│   ├── server/
│   │   └── server.c    # TCP server, one thread per client, cluster integration
│   ├── cli/
│   │   └── cli.c       # Interactive / one-shot client (readline support)
│   └── cluster/
│       ├── conf.h/c    # Cluster config parser
│       └── cluster.h/c # Mesh networking, heartbeat, replication, full sync
├── test/
│   └── run_test.sh     # End-to-end functional test suite
│   └── run_cluster_test.sh  # Cluster integration test
├── tests/
│   └── test_kvdb.c     # Unit tests
├── build/              # Build artifacts
├── Makefile
└── README.md
```

### 2.2 Core Engine (`kvdb.c`)

The data layer is fully decoupled from networking.

| Structure | Purpose |
|-----------|---------|
| `kv_entry` | One bucket: `key`, `value`, `state` (EMPTY, OCCUPIED, DELETED), **plus metadata** (`version`, `wallclock`, `origin`). |
| `kv_table` | Hash table: buckets array, metadata, optional `path`, `pthread_rwlock_t`, atomic logical clock. |

**Hash function**: FNV-1a 64-bit.
**Collision resolution**: quadratic probing (`idx = (h0 + i²) mod M`).
**Resize triggered**: when `load_factor > 0.75`; capacity doubles, entries are rehashed.

**Thread-safety model**:
* `SET` / `DEL` → `wrlock`
* `GET` / `EXISTS` / `KEYS` / `SAVE` → `rdlock`
* Each client runs in a detached `pthread`; locks guarantee serialisation of conflicting operations while allowing parallel reads.

**Conflict resolution (cluster mode)**:
Every write carries a monotonic `version` (Lamport-like), `wallclock`, and `origin` node-id. When a replica arrives, the database applies **Last-Write-Wins**: higher `version` wins; tie on `version` → higher `wallclock`; tie again → lexical `origin` ID.

### 2.3 Persistence Format

When a file path is supplied (`-f <file>`), `kv_save()` serialises the database to a compact binary format:

```
count          : size_t
for each key-value:
    key_len      : size_t
    key_bytes    : char[key_len]
    value_len    : size_t
    value_bytes  : char[value_len]
    version      : uint64_t
    wallclock    : uint64_t
    origin       : char[15]
```

On `kv_open()`, the file is read and every entry is re-inserted via `kv_set_meta()`, rebuilding the in-memory hash table with full metadata.

### 2.4 Networking (`server.c`)

* `accept()` loop in the main thread.
* Each accepted client is handed off to a **detached thread** (`client_thread`).
* A line-buffer accumulator (`rxbuf`) handles partial reads and multiple commands arriving in the same TCP segment.
* Commands and replies are terminated by a single newline (`\n`).
* `SIGPIPE` is ignored so that a broken client does not crash the server.

### 2.5 Client (`cli.c`)

* Opens a TCP socket to the server.
* **Non-interactive mode**: all positional arguments after `-h` / `-p` options are joined with spaces and sent as one command.
* **Interactive mode**: reads from `stdin`, sends the line, waits for one reply line, prints it, then shows `kvdb> ` again.
* Uses **GNU readline / libedit** for history, line editing, and graceful EOF (`Ctrl+D`).

---

## 3. Cluster Mode

`antzkv-server` can join a **replicated cluster** by supplying a configuration file with the node list. Any write (`SET`/`DEL`) is asynchronously propagated to all known peers.

### 3.1 Configuration

Create a `cluster.conf` file:

```conf
# One line per node
id=alpha host=192.168.1.10 port=6379:16380 replicate=disk
id=beta  host=192.168.1.11 port=6379:16380 replicate=memory
id=gamma host=192.168.1.12 port=6379:16380 replicate=auto
```

| Field | Description |
|-------|-------------|
| `id` | Unique node identifier (max 15 chars). Used in conflict resolution. |
| `host` | IP address or hostname. |
| `port` | `client_port:cluster_port`. `client_port` is for normal clients; `cluster_port` is the internal mesh bus. |
| `replicate` | `disk` = persists to `-f` file; `memory` = in-memory only; `auto` = inherits local `-f` setting. |

**Node self-discovery**: the server identifies its own entry by matching its startup client port (`-p`) against the `port=` field.

### 3.2 Replication Model

* **Topology**: full mesh. Every node connects to every other node.
* **Transport**: independent TCP connections on `cluster_port`.
* **Heartbeat**: every 500 ms; node marked dead after 2 s of silence.
* **Replication**: asynchronous. Writes are queued in a ring buffer and dispatched by a background thread.
* **Consistency**: eventual consistency with **Last-Write-Wins**.
* **Sync on join**: when an incoming peer is recognised, it can request a `SYNC_REQ`; the responding node streams the entire keyspace via `SNAPSHOT` / `SET` messages.
* **Node IDs**: each server persists a unique ID in `.nodeid.<port>` in the working directory.

### 3.3 Convenience scripts

Two helper scripts are provided:

| Script | Purpose |
|--------|---------|
| `start-cluster.sh` | Spawn multiple nodes automatically from a config file |
| `stop-cluster.sh`  | Gracefully kill all nodes started by `start-cluster.sh` |

#### `start-cluster.sh`

```bash
./start-cluster.sh [--memory|--disk] [NODES] [CONFIG_FILE]
```

| Flag / Arg | Default | Description |
|------------|---------|-------------|
| `--memory` | **default** | All nodes run in-memory only (no files) |
| `--disk`   |             | Each node persists to its own `<id>.db` file |
| `NODES`    | `3`         | Number of nodes to spawn |
| `CONFIG`   | `cluster.conf` | Path to the cluster config file (auto-generated if missing) |

**Examples**

```bash
# 3 nodes in-memory (default)
./start-cluster.sh

# 5 nodes with disk persistence
./start-cluster.sh --disk 5

# Custom config
./start-cluster.sh --memory 3 /path/to/my-cluster.conf
```

Output example:
```
Generated cluster.conf with 3 node(s) in memory mode
  Node 'alpha'  client=6301  cluster=17301
  Node 'beta'   client=6302  cluster=17302
  Node 'gamma'  client=6303  cluster=17303
All nodes started. Logs: logs/*.log
```

#### `stop-cluster.sh`

```bash
./stop-cluster.sh
```

Kills all servers whose PIDs were recorded by `start-cluster.sh`.

---

## 4. API / Wire Protocol

All interactions are **plain text over TCP**. Every command and every reply is a single line terminated by `\n` (the carriage return `\r`, if present, is stripped).

### Request Format

```
COMMAND arg1 arg2 ...\n
```

* `COMMAND` is case-insensitive.
* Arguments are separated by ASCII whitespace (` `, `\t`, `\r`).
* Maximum tokenised arguments per line: **4**.

### Reply Format

* Simple status: `OK`, `ERR`, `ERR unknown command`
* Value: raw string (for `GET`)
* Integer: decimal ASCII (`0`, `1`, `2`…)
* Missing value: `(nil)`
* Empty list: `(empty)`
* Healthcheck: `PONG`

### Example Session (raw TCP)

```text
Client -> Server:  SET temperature 23.5\n
Server -> Client:  OK\n
Client -> Server:  GET temperature\n
Server -> Client:  23.5\n
Client -> Server:  DEL temperature\n
Server -> Client:  1\n
Client -> Server:  GET temperature\n
Server -> Client:  (nil)\n
Client -> Server:  QUIT\n
Server -> Client:  OK\n
Server closes socket.
```

---

## 5. Build

Requirements:
* GCC or Clang with C11 support
* POSIX threads (`pthread`)
* GNU Make
* GNU readline / libedit (for the CLI)

```bash
cd /Users/antoniolatela/Documents/antz/kvdb
make
```

Output binaries:
* `build/antzkv-server` – compiled with cluster support (`-DCLUSTER_ENABLED`)
* `build/antzkv-cli`     – client with readline

Clean:
```bash
make clean
```

Run the test suites:
```bash
make test           # functional standalone tests
bash test/run_cluster_test.sh   # cluster integration tests
```

---

## 6. User Manual

### 6.1 Server Options

```bash
./build/antzkv-server [-p PORT] [-f FILE] [-c CLUSTER_CONF] [-C CLUSTER_PORT]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `-p PORT` | `6379` | TCP listening port for clients. |
| `-f FILE` | (none) | Optional binary persistence file. |
| `-c FILE` | (none) | **Cluster configuration file** (see §3.1). |
| `-C PORT` | `client_port + 10000` | Port for internal cluster bus. |

**Examples**

```bash
# Standalone, in-memory only
./build/antzkv-server

# With disk persistence
./build/antzkv-server -f /var/lib/antzkv.dat

# Cluster node alpha (disk)
./build/antzkv-server -p 6379 -f alpha.db -c cluster.conf -C 16380

# Cluster node beta (memory only)
./build/antzkv-server -p 6379 -c cluster.conf -C 16380
```

Stop the server with **`Ctrl+C`** (SIGINT). The database is automatically saved before exit when a file path is configured.

### 6.2 CLI Options

```bash
./build/antzkv-cli [-h HOST] [-p PORT] [COMMAND ...]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `-h HOST` | `127.0.0.1` | Server IP address. |
| `-p PORT` | `6379` | Server port. |

If `COMMAND` arguments are present, the client runs in **non-interactive mode**: it connects, sends the command, prints the reply, and exits.

If no command is given, the client starts an **interactive REPL**.

**Examples – non-interactive**

```bash
# Insert a key
./build/antzkv-cli SET user admin

# Read a key
./build/antzkv-cli GET user

# Check existence of two keys
./build/antzkv-cli EXISTS user config

# Persist to disk explicitly
./build/antzkv-cli SAVE

# Remote server
./build/antzkv-cli -h 10.0.0.5 -p 4000 PING
```

**Example – interactive**

```bash
./build/antzkv-cli
```

```text
Connesso a 127.0.0.1:6379.
Digita i comandi (QUIT per uscire).
kvdb> SET name Alice
OK
kvdb> GET name
Alice
kvdb> KEYS
name
kvdb> DEL name
1
kvdb> GET name
(nil)
kvdb> QUIT
OK
```

### 6.3 Command Reference

| Command | Args | Description | Reply |
|---------|------|-------------|-------|
| `SET` | `key value` | Store `value` under `key`. Overwrites existing values. | `OK` or `ERR` |
| `GET` | `key` | Retrieve value associated with `key`. | Value string, or `(nil)` |
| `DEL` | `key [key ...]` | Delete one or more keys. | Integer: count of keys actually removed |
| `EXISTS` | `key [key ...]` | Check existence of keys. | Integer: count of existing keys |
| `KEYS` | — | Return all keys in the database. | Space-separated list, or `(empty)` |
| `SAVE` | — | Flush the database to the configured file. | `OK` or `ERR` (if no file configured) |
| `PING` | — | Health / latency check. | `PONG` |
| `QUIT` | — | Ask the server to close the connection. | `OK` |

---

## 7. Testing

### Standalone tests
A complete Bash-driven functional test suite lives in `test/run_test.sh`. It exercises:

* Basic CRUD (`SET`, `GET`, `DEL`)
* Overwrite semantics
* Key existence checks
* Enumeration (`KEYS`)
* Missing-key handling
* `SAVE` failure when persistence is disabled
* Concurrent stress test (20 parallel `SET`s immediately followed by `GET`s)
* Full persistence cycle (write, save, restart, verify)

```bash
make test
```

### Cluster tests
The script `test/run_cluster_test.sh` spawns a two-node local cluster, performs writes on one node, and verifies replication on the other (including bidirectional traffic and `DEL` propagation).

```bash
bash test/run_cluster_test.sh
```

### Unit tests
```bash
gcc -O2 -Wall -Wextra -Iinclude -pthread tests/test_kvdb.c build/core/kvdb.o -o build/test_kvdb
./build/test_kvdb
```

---

## 8. License

This project is released for internal use. Modify and redistribute freely.

---

## 8. Client Libraries

Native **thread-safe** client libraries are provided for **Python** and **Java**. They use pure TCP sockets — no CLI subprocesses.

### 8.1 Python

```python
from antzkv import AntzKVClient

with AntzKVClient("127.0.0.1", 6379) as client:
    assert client.set("name", "Alice")
    print(client.get("name"))          # Alice
    print(client.delete("name"))       # 1
    print(client.keys())               # []
```

**Pipeline (batch operations)** — recommended for high throughput:

```python
pairs = [(f"k{i}", f"v{i}") for i in range(100)]
results = client.pipeline_set(pairs)   # [True, True, ...]

keys = [f"k{i}" for i in range(100)]
values = client.pipeline_get(keys)      # ["v0", "v1", ...]
```

| Method | Description |
|--------|-------------|
| `set(key, value)` | Single SET |
| `get(key)` | Single GET (None if missing) |
| `delete(*keys)` | Delete keys, returns count |
| `exists(*keys)` | Check existence, returns count |
| `keys()` | Return all keys |
| `pipeline_set(pairs)` | Batch SET list of (key, value) tuples |
| `pipeline_get(keys)` | Batch GET list of keys |
| `pipeline_delete(keys)` | Batch DELETE list of keys |

**Location:** `clients/python/antzkv/__init__.py`

### 8.2 Java

```java
try (AntzKVClient client = new AntzKVClient("127.0.0.1", 6379)) {
    client.connect();
    client.set("name", "Alice");
    System.out.println(client.get("name"));
    client.delete("name");
}
```

**Connection pool:**

```java
try (AntzKVClient.Pool pool = new AntzKVClient.Pool("127.0.0.1", 6379, 4)) {
    AntzKVClient c = pool.acquire();
    c.set("key", "value");
    pool.release(c);
}
```

| Method | Description |
|--------|-------------|
| `set(key, value)` | Single SET |
| `get(key)` | Single GET (null if missing) |
| `delete(keys...)` | Delete keys, returns count |
| `exists(keys...)` | Check existence, returns count |
| `keys()` | Return all keys |

**Location:** `clients/java/src/main/java/com/antzkv/AntzKVClient.java`

---

## 9. Performance

### Benchmark Results

| Scenario | Throughput | Latency p99 |
|----------|-----------|-------------|
| Sync 1:1 (default client) | **~497 TPS** | ~2.8ms |
| Pipeline batch=50 (Python) | **~56,000 TPS** | ~5ms |
| Raw socket pipeline (theoretical max) | **~87,000 TPS** | ~2ms |

### How to reproduce

```bash
# Start a standalone server
./build/antzkv-server -p 6301

# Quick benchmark
python3 quick_bench.py
```

Expected output:
```
Pipeline SET: 5000 ops in 0.089s = 55,972 TPS
Pipeline GET: 1000 ops in 0.013s = 74,582 TPS
```

### Bottlenecks & Roadmap

| # | Bottleneck | Fix | Priority |
|---|-----------|-----|----------|
| 1 | Protocol sync 1:1 | ✅ Pipeline client (DONE) | High |
| 2 | Thread creation per client | Thread pool server | High |
| 3 | Global hash table lock | Sharded hash table (16 shards) | High |
| 4 | Memory allocation | Arena allocator | Medium |
| 5 | Replication lock | Lock-free SPSC ring buffer | Medium |

**Full report:** `PERFORMANCE_REPORT.md`

---

*Branch: `cluster` — cluster support, async replication, LWW conflict resolution.*
