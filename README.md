# antzkv

`antzkv` is a high-performance, in-memory / file-persisted **key-value database** inspired by Redis, written in portable **C11** with POSIX threads. It supports concurrent client access over TCP, **RESP protocol** (Redis-compatible), a rich set of data structures, **Pub/Sub**, key expiry with active eviction, and both interactive and non-interactive CLI modes.

**Cluster mode:** multi-node replication with automatic mesh networking, Last-Write-Wins conflict resolution, and configurable per-node persistence.

[![CI](https://github.com/Antonius74/antzkv/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Antonius74/antzkv/actions/workflows/ci.yml)

---

## Table of Contents

1. [Technical Overview](#1-technical-overview)
2. [Architecture](#2-architecture)
3. [Data Structures](#3-data-structures)
4. [Cluster Mode](#4-cluster-mode)
5. [API / Wire Protocol](#5-api--wire-protocol)
6. [Build](#6-build)
7. [User Manual](#7-user-manual)
8. [Testing](#8-testing)
9. [Client Libraries](#9-client-libraries)
10. [Performance](#10-performance)
11. [License](#11-license)

---

## 1. Technical Overview

| Feature | Detail |
|---------|--------|
| **Language** | C11 (POSIX.1-2008) |
| **Concurrency** | Read-write lock on key-space table + **pre-allocated thread pool** (8 workers) |
| **Storage model** | Open-addressing hash table (key-space) with quadratic probing + polymorphic `db_object_t` model |
| **Data structures** | Strings, Lists, Sets, Hashes, Sorted Sets (skip list) |
| **Persistence** | Binary snapshot file (custom format) |
| **Protocol** | **RESP** (Redis Serialization Protocol) — Simple Strings, Errors, Integers, Bulk Strings, Arrays |
| **Key expiry** | Per-key TTL with background active eviction thread |
| **Pub/Sub** | Channel-based publish/subscribe with RESP-formatted `message` notifications |
| **Default port** | `6379` |
| **Clustering** | Mesh TCP overlay, heartbeat-based failure detection, async replication |

---

## 2. Architecture

### 2.1 Directory Layout

```
.
├── include/
│   └── kvdb.h          # Public C API — object model, data structures, PubSub
├── src/
│   ├── core/
│   │   └── kvdb.c      # Object store, hash table, persistence, TTL, skip list, PubSub
│   ├── server/
│   │   └── server.c    # TCP server with thread pool, RESP protocol, all commands
│   ├── cli/
│   │   └── cli.c       # Interactive / one-shot client (RESP-aware, readline support)
│   └── cluster/
│       ├── conf.h/c    # Cluster config parser
│       └── cluster.h/c # Mesh networking, heartbeat, replication, full sync
├── test/
│   ├── run_test.sh     # End-to-end functional test suite (57 tests)
│   └── run_cluster_test.sh  # Cluster integration test
├── tests/
│   └── test_kvdb.c     # C unit tests (library-level)
├── clients/
│   ├── python/         # Python client v2.0 (thread-safe, pipeline, pool)
│   └── java/           # Java client (thread-safe, connection pool)
├── build/              # Build artifacts
├── Makefile
└── README.md
```

### 2.2 Object Model

Every value stored in antzkv is a **polymorphic `db_object_t`**:

```c
typedef struct db_object {
    int       type;        // OBJ_STRING | OBJ_LIST | OBJ_SET | OBJ_HASH | OBJ_ZSET
    void     *ptr;         // concrete data-structure pointer
    int64_t   ttl;         // -1 = no expiry, >=0 = unix-msec expiry time
    uint64_t  version;     // Lamport-like clock (LWW)
    uint64_t  wallclock;   // Unix microsecond timestamp
    char      origin[16];  // Node ID for conflict resolution
} db_object_t;
```

### 2.3 Core Engine (`kvdb.c`)

The key-space is a flat open-addressing hash table mapping `char*` keys to `db_object_t*` values.

| Detail | Implementation |
|--------|---------------|
| **Hash function** | FNV-1a 64-bit |
| **Collision resolution** | Quadratic probing (`idx = (h0 + i²) mod M`) |
| **Load factor** | Resize (doubles capacity) when `count/capacity > 0.75` |
| **Initial capacity** | 32 buckets |
| **Entry states** | `EMPTY`, `OCCUPIED`, `DELETED` |
| **Thread safety** | `pthread_rwlock_t` — `wrlock` for SET/DEL, `rdlock` for GET/EXISTS/KEYS/SAVE |
| **Versioning** | Atomic monotonic counter (`pthread_mutex_t`) |

**Conflict resolution (cluster mode):**
Every write carries `version`, `wallclock`, and `origin` node-id. The database applies **Last-Write-Wins**: higher `version` wins; tie → higher `wallclock`; tie → lexical `origin` ID.

### 2.4 Networking (`server.c`)

* `accept()` loop in the main thread.
* Accepted connections are dispatched to a **pre-allocated thread pool** (8 worker threads) via a producer-consumer queue with `pthread_cond_t`.
* A line-buffer accumulator handles partial reads and multiple pipelined commands in the same TCP segment.
* **RESP protocol** replies: `+OK\r\n`, `-ERR\r\n`, `:N\r\n`, `$N\r\n...\r\n`, `*N\r\n...`
* `SIGPIPE` is ignored; `SIGINT` / `SIGTERM` trigger graceful shutdown with automatic SAVE.

### 2.5 CLI Client (`cli.c`)

* Opens a TCP socket to the server.
* **Non-interactive mode**: positional arguments are joined and sent as one command; RESP reply is parsed and printed.
* **Interactive mode**: uses `readline()` from libedit (macOS) or GNU readline (Linux) with command history, `kvdb> ` prompt, and `Ctrl+D` (EOF) handling.
* Full RESP decoding for all reply types.

### 2.6 Key Expiry

* Per-key TTL stored as **absolute Unix-millisecond expiry** in `obj->ttl`.
* Supported commands: `EXPIRE`, `TTL`, `PTTL`, `PERSIST`, `SET ... EX|PX`.
* **Active eviction thread** runs every 100ms, sampling 20 random keys and deleting expired ones.
* Lazy expiry: accessed keys are checked via `kv_active_expire()`.

---

## 3. Data Structures

### 3.1 Strings

String values are stored as heap-allocated `char*` inside `OBJ_STRING` objects.

| Command | Description |
|---------|-------------|
| `SET key value [EX sec\|PX ms]` | Store value with optional TTL |
| `SETNX key value` | Set only if key does not exist |
| `SETEX key seconds value` | Set with expiry in seconds |
| `GET key` | Retrieve value |
| `GETRANGE key start end` | Substring (negative indices supported) |
| `APPEND key value` | Append to value, returns new length |
| `STRLEN key` | String length |
| `INCR key` | Increment integer by 1 |
| `DECR key` | Decrement integer by 1 |
| `INCRBY key delta` | Increment by delta |
| `DECRBY key delta` | Decrement by delta |

### 3.2 Lists

Doubly-linked list (`OBJ_LIST`). O(1) push/pop at both ends, O(N) index operations.

| Command | Description |
|---------|-------------|
| `LPUSH key value [value ...]` | Prepend elements |
| `RPUSH key value [value ...]` | Append elements |
| `LPOP key` | Remove and return head |
| `RPOP key` | Remove and return tail |
| `LLEN key` | List length |
| `LINDEX key index` | Element at position (0-based, negative from end) |
| `LSET key index value` | Set element at position |
| `LREM key count value` | Remove occurrences of value |
| `RPOPLPUSH src dst` | RPOP from src, LPUSH to dst |

### 3.3 Sets

Hash-table-based set (`OBJ_SET`) using closed-addressing for members. O(1) add/remove/check.

| Command | Description |
|---------|-------------|
| `SADD key member [member ...]` | Add members, returns count added |
| `SREM key member [member ...]` | Remove members, returns count removed |
| `SISMEMBER key member` | Check membership |
| `SCARD key` | Cardinality |
| `SMEMBERS key` | Return all members |

### 3.4 Hashes

Hash-table-based map (`OBJ_HASH`) mapping field → value. O(1) insert/lookup.

| Command | Description |
|---------|-------------|
| `HSET key field value` | Set field |
| `HGET key field` | Get field value |
| `HDEL key field [field ...]` | Delete fields |
| `HEXISTS key field` | Check field existence |
| `HLEN key` | Number of fields |
| `HKEYS key` | All field names |
| `HVALS key` | All field values |
| `HGETALL key` | All field-value pairs (flat array) |

### 3.5 Sorted Sets

**Skip list** (`OBJ_ZSET`) with 12 levels and power-law (p=1/4) randomization. O(log N) insert/delete.

| Command | Description |
|---------|-------------|
| `ZADD key score member [score member ...]` | Add with score |
| `ZREM key member [member ...]` | Remove members |
| `ZSCORE key member` | Get score |
| `ZCARD key` | Cardinality |
| `ZRANK key member` | 0-based ascending rank |
| `ZREVRANK key member` | 0-based descending rank |
| `ZRANGE key start stop` | Range by ascending rank |
| `ZREVRANGE key start stop` | Range by descending rank |

---

## 4. Cluster Mode

`antzkv-server` can join a **replicated cluster** by supplying a configuration file with the node list. Any write (`SET`/`DEL`) is asynchronously propagated to all known peers.

### 4.1 Configuration

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

### 4.2 Replication Model

* **Topology**: full mesh. Every node connects to every other node.
* **Transport**: independent TCP connections on `cluster_port`.
* **Heartbeat**: every 500 ms; node marked dead after 2 s of silence.
* **Replication**: asynchronous. Writes are queued in a ring buffer and dispatched by a background thread.
* **Consistency**: eventual consistency with **Last-Write-Wins**.
* **Sync on join**: when an incoming peer is recognised, it can request a `SYNC_REQ`; the responding node streams the entire keyspace via `SNAPSHOT` / `SET` messages.
* **Node IDs**: each server persists a unique ID in `.nodeid.<port>` in the working directory.

### ⚠️ Cluster Replication Status

The cluster networking module (`src/cluster/cluster.c`) establishes TCP mesh connections between nodes, but **cross-node data replication is currently non-functional**. This is a known architectural bug.

**What works:**
- Multiple nodes start successfully
- Heartbeat messages are exchanged
- The mesh TCP overlay is active
- Data persists on individual nodes

**What does NOT work:**
- Automatic replication of SET/DEL from one node to another
- SYNC_REQ / SNAPSHOT full sync
- Last-Write-Wins conflict resolution across nodes

**Workaround:** For multi-node read scaling, deploy a load balancer (e.g., HAProxy, Nginx) in front of individual nodes and write to one node (or use client-side replication).

### 4.3 Convenience Scripts

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

#### `stop-cluster.sh`

```bash
./stop-cluster.sh
```

Kills all servers whose PIDs were recorded by `start-cluster.sh`.

---

## 5. API / Wire Protocol

antzkv uses **RESP (REdis Serialization Protocol)** over TCP.

### 5.1 Request Format

```
COMMAND arg1 arg2 ...\n
```

* `COMMAND` is case-insensitive.
* Arguments are separated by ASCII whitespace.
* Up to 64 tokenized arguments per line.

### 5.2 Reply Format (RESP)

| Prefix | Type | Example |
|--------|------|---------|
| `+` | Simple String | `+OK\r\n` |
| `-` | Error | `-ERR unknown command\r\n` |
| `:` | Integer | `:42\r\n` |
| `$` | Bulk String | `$5\r\nhello\r\n` |
| `*` | Array | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

### 5.3 Example Session (raw TCP)

```text
Client -> Server:  SET temperature 23.5\n
Server -> Client:  +OK\r\n
Client -> Server:  GET temperature\n
Server -> Client:  $4\r\n23.5\r\n
Client -> Server:  DEL temperature\n
Server -> Client:  :1\r\n
Client -> Server:  GET temperature\n
Server -> Client:  $-1\r\n
Client -> Server:  QUIT\n
Server -> Client:  +OK\r\n
Server closes socket.
```

### 5.4 Pub/Sub Format

When a message is published, subscribers receive RESP-formatted messages:

```text
*3\r\n
$7\r\nmessage\r\n
$7\r\nchannel\r\n
$5\r\nhello\r\n
```

---

## 6. Build

Requirements:
* GCC or Clang with C11 support
* POSIX threads (`pthread`)
* GNU Make
* GNU readline / libedit (for the CLI)

```bash
make
```

Output binaries:
* `build/antzkv-server` — compiled with cluster support (`-DCLUSTER_ENABLED`)
* `build/antzkv-cli`     — client with readline

Clean:
```bash
make clean
```

---

## 7. User Manual

### 7.1 Server Options

```bash
./build/antzkv-server [-p PORT] [-f FILE] [-c CLUSTER_CONF] [-C CLUSTER_PORT]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `-p PORT` | `6379` | TCP listening port for clients. |
| `-f FILE` | (none) | Optional binary persistence file. |
| `-c FILE` | (none) | Cluster configuration file (see §4.1). |
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

### 7.2 CLI Options

```bash
./build/antzkv-cli [-h HOST] [-p PORT] [COMMAND ...]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `-h HOST` | `127.0.0.1` | Server IP address. |
| `-p PORT` | `6379` | Server port. |

**Examples – non-interactive**

```bash
./build/antzkv-cli SET user admin
./build/antzkv-cli GET user
./build/antzkv-cli EXISTS user config
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
kvdb> LPUSH queue a b c
3
kvdb> LPOP queue
c
kvdb> HGETALL myhash
f1
v1
kvdb> ZADD z 1.0 x 2.0 y
2
kvdb> ZRANGE z 0 -1
x y
kvdb> QUIT
OK
```

### 7.3 Command Reference

#### Generic Key-Space

| Command | Description | Reply |
|---------|-------------|-------|
| `SET key value [EX s\|PX ms]` | Store value | `OK` |
| `SETNX key value` | Set if not exists | `1` or `0` |
| `SETEX key sec value` | Set with TTL | `OK` |
| `GET key` | Retrieve value | Bulk string or `$-1` |
| `DEL key [key ...]` | Delete keys | Count deleted |
| `EXISTS key [key ...]` | Check existence | Count existing |
| `TYPE key` | Object type | `string`, `list`, `set`, `hash`, `zset`, `none` |
| `KEYS` | All keys | Array of bulk strings |
| `SAVE` | Flush to file | `OK` or `-ERR` |
| `PING [message]` | Health check | `PONG` or bulk string |
| `QUIT` | Close connection | `OK` |

#### TTL

| Command | Description | Reply |
|---------|-------------|-------|
| `EXPIRE key seconds` | Set TTL | `1` (set) or `0` (no key) |
| `TTL key` | Remaining seconds | `-1` (persistent), `-2` (missing), or N |
| `PTTL key` | Remaining milliseconds | `-1`, `-2`, or N |
| `PERSIST key` | Remove TTL | `1` or `0` |

#### String Operations

| Command | Description | Reply |
|---------|-------------|-------|
| `GETRANGE key start end` | Substring | Bulk string |
| `APPEND key value` | Append to string | New length |
| `STRLEN key` | String length | Integer |
| `INCR key` | Increment by 1 | New value |
| `DECR key` | Decrement by 1 | New value |
| `INCRBY key N` | Increment by N | New value |
| `DECRBY key N` | Decrement by N | New value |

#### Lists

| Command | Reply |
|---------|-------|
| `LPUSH key v [v ...]` | New list length |
| `RPUSH key v [v ...]` | New list length |
| `LPOP key` | Bulk string or `$-1` |
| `RPOP key` | Bulk string or `$-1` |
| `LLEN key` | Integer |
| `LINDEX key idx` | Bulk string or `$-1` |
| `LSET key idx value` | `OK` or error |
| `LREM key count value` | Count removed |
| `RPOPLPUSH src dst` | Bulk string or `$-1` |

#### Sets

| Command | Reply |
|---------|-------|
| `SADD key m [m ...]` | Count added |
| `SREM key m [m ...]` | Count removed |
| `SISMEMBER key m` | `1` or `0` |
| `SCARD key` | Cardinality |
| `SMEMBERS key` | Array of bulk strings |

#### Hashes

| Command | Reply |
|---------|-------|
| `HSET key field value` | `1` (created) or `0` (updated) |
| `HGET key field` | Bulk string or `$-1` |
| `HDEL key f [f ...]` | Count deleted |
| `HEXISTS key field` | `1` or `0` |
| `HLEN key` | Number of fields |
| `HKEYS key` | Array of field names |
| `HVALS key` | Array of field values |
| `HGETALL key` | Flat array [f1,v1,f2,v2,...] |

#### Sorted Sets

| Command | Reply |
|---------|-------|
| `ZADD key sc m [sc m ...]` | Count added |
| `ZREM key m [m ...]` | Count removed |
| `ZSCORE key m` | Bulk string (score) or `$-1` |
| `ZCARD key` | Cardinality |
| `ZRANK key m` | Integer (0-based) or `-1` |
| `ZREVRANK key m` | Integer (0-based) or `-1` |
| `ZRANGE key start stop` | Array of members |
| `ZREVRANGE key start stop` | Array of members |

#### Pub/Sub

| Command | Description |
|---------|-------------|
| `PUBLISH channel message` | Send to channel, returns subscriber count |
| `SUBSCRIBE channel [channel ...]` | Subscribe, enters Pub/Sub mode |
| `UNSUBSCRIBE [channel ...]` | Unsubscribe |

---

## 8. Testing

### 8.1 Functional Tests

A complete Bash-driven functional test suite lives in `test/run_test.sh`. It exercises:

* PING, QUIT, SAVE
* Basic CRUD (SET/GET/DEL/EXISTS) with overwrite and missing key semantics
* All data structures: Lists (LPUSH/RPUSH/LPOP/RPOP/LLEN/LINDEX/LSET/LREM/RPOPLPUSH)
* Sets (SADD/SISMEMBER/SCARD/SMEMBERS)
* Hashes (HSET/HGET/HEXISTS/HLEN/HKEYS/HVALS/HGETALL)
* Sorted Sets (ZADD/ZCARD/ZRANK/ZREVRANK/ZRANGE/ZREVRANGE/ZSCORE)
* TTL (EXPIRE/TTL/PTTL/PERSIST) and expiry
* String operations (INCR/DECR/INCRBY/DECRBY/APPEND/STRLEN/GETRANGE/SETNX/SETEX)
* Pub/Sub (PUBLISH)
* Concurrent writes (10 sequential CLI instances, verified)
* Full persistence cycle (write → save → restart → load → verify)

```bash
make test
```

### 8.2 Cluster Tests

```bash
bash test/run_cluster_test.sh
```

### 8.3 C Unit Tests

```bash
gcc -O2 -Wall -Wextra -Iinclude -pthread tests/test_kvdb.c build/core/kvdb.o -o build/test_kvdb
./build/test_kvdb
```

---

## 9. Client Libraries

Native **thread-safe** client libraries are provided for **Python** and **Java**. They use pure TCP sockets — no CLI subprocesses.

### 9.1 Python

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

### 9.2 Java

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

**Location:** `clients/java/src/main/java/com/antzkv/AntzKVClient.java`

---

## 10. Performance

### Benchmark Results

| Scenario | Throughput | Latency p99 |
|----------|-----------|-------------|
| Sync 1:1 (default client) | **~497 TPS** | ~2.8ms |
| Pipeline batch=50 (Python) | **~56,000 TPS** | ~5ms |
| Raw socket pipeline (theoretical max) | **~87,000 TPS** | ~2ms |

### How to reproduce

```bash
./build/antzkv-server -p 6301
python3 quick_bench.py
```

### Bottlenecks & Roadmap (Solved)

| # | Bottleneck | Status |
|---|-----------|--------|
| 1 | Protocol sync 1:1 | ✅ Pipeline client + RESP |
| 2 | Thread creation per client | ✅ Thread pool (8 workers) |
| 3 | Global hash table lock | 🔜 Sharded hash table |
| 4 | Memory allocation | 🔜 Arena allocator |
| 5 | Replication lock | 🔜 Lock-free SPSC ring buffer |

**Full report:** `PERFORMANCE_REPORT.md`

---

## 11. License

This project is released for internal use. Modify and redistribute freely.
