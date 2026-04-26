#!/usr/bin/env python3
"""Stress test with pipelining (batch SETs)."""
import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "clients", "python"))
from antzkv import AntzKVPool

NODES = [6301, 6302, 6303, 6304, 6305]
WORKERS_PER_NODE = 4   # 20 worker totali
BATCH = 50             # 50 SET per network roundtrip
DURATION = 30

counts = [0] * len(NODES)
errors = [0] * len(NODES)
latencies = [[] for _ in NODES]
stop = threading.Event()

def worker(node_idx, wid):
    c = AntzKVPool("127.0.0.1", NODES[node_idx], size=2)
    local = 0
    local_err = 0
    local_lat = []
    while not stop.is_set():
        t0 = time.perf_counter()
        try:
            conn = c.acquire()
            pair = []
            for i in range(BATCH):
                pair.append((f"k{wid}_{local+i}", f"v{local+i}"))
            replies = conn.pipeline_set(pair)
            c.release(conn)
            ok = sum(replies)
            local += ok
            local_err += (BATCH - ok)
        except Exception as e:
            local_err += BATCH
        t1 = time.perf_counter()
        local_lat.append((t1 - t0) * 1000.0)
        if local % 1000 == 0:
            with metrics_lock:
                counts[node_idx] += local
                errors[node_idx] += local_err
                latencies[node_idx].extend(local_lat)
                if len(latencies[node_idx]) > 10000:
                    latencies[node_idx] = latencies[node_idx][-10000:]
            local = 0
            local_err = 0
            local_lat = []
    with metrics_lock:
        counts[node_idx] += local
        errors[node_idx] += local_err
        latencies[node_idx].extend(local_lat)
    c.close()

metrics_lock = threading.Lock()
threads = []
for ni in range(len(NODES)):
    for w in range(WORKERS_PER_NODE):
        t = threading.Thread(target=worker, args=(ni, ni * 100 + w))
        t.daemon = True
        t.start()
        threads.append(t)

print(f"Pipelined stress: {NODES} nodes × {WORKERS_PER_NODE} workers, batch={BATCH}, {DURATION}s")
start = time.time()
time.sleep(DURATION)
stop.set()
for t in threads:
    t.join(timeout=5)

elapsed = time.time() - start
with metrics_lock:
    total = sum(counts)
    total_err = sum(errors)
    all_lat = []
    for l in latencies:
        all_lat.extend(l)

all_lat.sort()
p50 = all_lat[len(all_lat)//2] if all_lat else 0
p90 = all_lat[int(len(all_lat)*0.90)] if all_lat else 0
p99 = all_lat[int(len(all_lat)*0.99)] if all_lat else 0

print(f"\n=== PIPELINE RESULTS ({elapsed:.1f}s) ===")
print(f"Total SET:    {total:,}")
print(f"Errors:       {total_err}")
print(f"TPS:          {total/elapsed:,.0f}")
print(f"Latency:      p50={p50:.3f}ms  p90={p90:.3f}ms  p99={p99:.3f}ms")
print(f"Per-node:     ", end="")
for i in range(len(NODES)):
    print(f"N{i+1}={counts[i]:,}  ", end="")
print()
