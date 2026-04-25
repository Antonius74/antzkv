# antzkv

`antzkv` is a lightweight, in-memory / file-persisted **key-value database** inspired by Redis, written in portable **C11** with POSIX threads. It supports concurrent client access over TCP, a simple text-line protocol, and both interactive and non-interactive CLI modes.

---

## Table of Contents

1. [Technical Overview](#1-technical-overview)
2. [Architecture](#2-architecture)
3. [API / Wire Protocol](#3-api--wire-protocol)
4. [Build](#4-build)
5. [User Manual](#5-user-manual)
   1. [Server Options](#51-server-options)
   2. [CLI Options](#52-cli-options)
   3. [Command Reference](#53-command-reference)
6. [Testing](#6-testing)
7. [License](#7-license)

---

## 1. Technical Overview

* **Language**: C11 (POSIX.1-2008)
* **Concurrency**: read-write lock (`pthread_rwlock_t`) on the internal hash table + one `pthread` per accepted client.
* **Storage model**: open-addressing hash table with quadratic probing.
* **Persistence**: optional append-rewrite style binary file (custom format).
* **Protocol**: plain TCP, newline-terminated text commands and replies.
* **Default port**: `6379`

---

## 2. Architecture

### 2.1 Directory Layout

```
.
├── include/
│   └── kvdb.h          # Public C API
├── src/
│   ├── core/
│   │   └── kvdb.c      # Hash table + persistence engine
│   ├── server/
│   │   └── server.c    # TCP server, one thread per client
│   └── cli/
│       └── cli.c       # Interactive / one-shot client
├── test/
│   └── run_test.sh     # End-to-end functional test suite
├── build/              # Build artifacts
├── Makefile
└── README.md
```

### 2.2 Core Engine (`kvdb.c`)

The data layer is fully decoupled from networking.

| Structure | Purpose |
|-----------|---------|
| `kv_entry` | One bucket: `key`, `value`, `state` (EMPTY, OCCUPIED, DELETED). |
| `kv_table` | Hash table: buckets array, metadata, optional `path`, `pthread_rwlock_t`. |

**Hash function**: FNV-1a 64-bit.
**Collision resolution**: quadratic probing (`idx = (h0 + i²) mod M`).
**Resize triggered**: when `load_factor > 0.75`; capacity doubles, entries are rehashed.

**Thread-safety model**:
* `SET` / `DEL` → `wrlock`
* `GET` / `EXISTS` / `KEYS` / `SAVE` → `rdlock`
* Each client runs in a detached `pthread`; locks guarantee serialisation of conflicting operations while allowing parallel reads.

### 2.3 Persistence Format

When a file path is supplied (`-f <file>`), `kv_save()` serialises the database to a compact binary format:

```
count          : size_t
for each key-value:
    key_len      : size_t
    key_bytes    : char[key_len]
    value_len    : size_t
    value_bytes  : char[value_len]
```

On `kv_open()`, the file is read and every entry is re-inserted via `kv_set()`, rebuilding the in-memory hash table.

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

---

## 3. API / Wire Protocol

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

## 4. Build

Requirements:
* GCC or Clang with C11 support
* POSIX threads (`pthread`)
* GNU Make

```bash
cd /Users/antoniolatela/Documents/antz/kvdb
make
```

Output binaries:
* `build/antzkv-server`
* `build/antzkv-cli`

Clean:
```bash
make clean
```

Run the test suite:
```bash
make test
```

---

## 5. User Manual

### 5.1 Server Options

```bash
./build/antzkv-server [-p PORT] [-f FILE]
```

| Option | Default | Meaning |
|--------|---------|---------|
| `-p PORT` | `6379` | TCP listening port. |
| `-f FILE` | (none) | Optional binary persistence file. If the file exists it is loaded on startup; it is overwritten on `SAVE` or graceful shutdown. |

**Examples**

```bash
# In-memory only
./build/antzkv-server

# Listen on port 4000 with disk persistence
./build/antzkv-server -p 4000 -f /var/lib/antzkv.dat
```

Stop the server with **`Ctrl+C`** (SIGINT). The database is automatically saved before exit when a file path is configured.

### 5.2 CLI Options

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
Connesso a 127.0.0.1:6379. Digita i comandi (QUIT per uscire).
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

### 5.3 Command Reference

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

## 6. Testing

A complete Bash-driven functional test suite lives in `test/run_test.sh`. It exercises:

* Basic CRUD (`SET`, `GET`, `DEL`)
* Overwrite semantics
* Key existence checks
* Enumeration (`KEYS`)
* Missing-key handling
* `SAVE` failure when persistence is disabled
* Concurrent stress test (20 parallel `SET`s immediately followed by `GET`s)
* Full persistence cycle (write, save, restart, verify)

All tests pass with zero errors:
```bash
make test
```

---

## 7. License

This project is released for internal use. Modify and redistribute freely.
