#!/usr/bin/env python3
"""
High-throughput benchmark for antzkv cluster.
"""

import sys
import time
import threading
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "clients", "python"))

from antzkv import AntzKVPool

HOST = "127.0.0.1"
NODES = [6301, 6302, 6303, 6304, 6305]
NUM_WORKERS = 20       # thread totali (4 per nodo)
POOL_SIZE = 2
DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 60
TX_TARGET = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000

metrics_lock = threading.Lock()
total_tx = 0
total_err = 0
latencies = []
start_time = time.time()
stop_event = threading.Event()

def worker(node_port, wid):
    global total_tx, total_err, latencies
    pool = AntzKVPool(HOST, node_port, size=POOL_SIZE)
    local_tx = 0
    local_err = 0
    local_lat = []
    try:
        while not stop_event.is_set():
            key = f"k{wid}_{local_tx}"
            val = f"v{local_tx}"
            t0 = time.perf_counter()
            try:
                c = pool.acquire()
                ok = c.set(key, val)
                pool.release(c)
                if not ok:
                    local_err += 1
            except Exception:
                local_err += 1
                time.sleep(0.001)
            t1 = time.perf_counter()
            local_tx += 1
            local_lat.append((t1 - t0) * 1000.0)
            if local_tx % 1000 == 0:
                with metrics_lock:
                    total_tx += local_tx
                    total_err += local_err
                    latencies.extend(local_lat)
                    if len(latencies) > 10000:
                        latencies = latencies[-10000:]
                local_tx = 0
                local_err = 0
                local_lat.clear()
    finally:
        with metrics_lock:
            total_tx += local_tx
            total_err += local_err
            latencies.extend(local_lat)
        pool.close()

def monitor():
    while not stop_event.is_set():
        time.sleep(5)
        with metrics_lock:
            tx = total_tx
            err = total_err
        elapsed = time.time() - start_time
        tps = tx / elapsed if elapsed > 0 else 0
        print(f"[{elapsed:.0f}s] Tx={tx:,} TPS={tps:,.0f} Err={err}")

def main():
    print(f"Benchmark: {len(NODES)} nodes, {NUM_WORKERS} workers, {DURATION}s, target {TX_TARGET:,} tx")
    threads = []
    for port in NODES:
        for w in range(NUM_WORKERS // len(NODES)):
            t = threading.Thread(target=worker, args=(port, port * 100 + w))
            t.daemon = True
            t.start()
            threads.append(t)

    threading.Thread(target=monitor, daemon=True).start()
    time.sleep(DURATION)
    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    elapsed = time.time() - start_time
    with metrics_lock:
        tx = total_tx
        err = total_err
        lats = sorted(latencies)
    p50 = lats[len(lats)//2] if lats else 0
    p90 = lats[int(len(lats)*0.90)] if lats else 0
    p99 = lats[int(len(lats)*0.99)] if lats else 0

    print(f"\n=== RESULTS ===")
    print(f"Tx={tx:,} Err={err} TPS={tx/elapsed:,.0f}")
    print(f"Lat ms p50={p50:.3f} p90={p90:.3f} p99={p99:.3f}")
    print(f"Target met: {'YES' if tx >= TX_TARGET else 'NO'} ({tx/TX_TARGET*100:.1f}%)")

if __name__ == "__main__":
    main()
