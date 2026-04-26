#!/usr/bin/env python3
"""
Stress test rapido: 30s, più thread possibile per misurare throughput max.
"""
import sys, os, time, threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "clients", "python"))
from antzkv import AntzKVClient

NODES = [6301, 6302, 6303, 6304, 6305]
WORKERS_PER_NODE = 8   # 40 worker totali
DURATION = 30

counts = [0] * len(NODES)
errors = [0] * len(NODES)
latencies = [[] for _ in NODES]
stop = threading.Event()

def worker(node_idx, wid):
    c = AntzKVClient("127.0.0.1", NODES[node_idx])
    try:
        c.connect()
    except Exception:
        errors[node_idx] += 1
        return
    local = 0
    local_err = 0
    local_lat = []
    while not stop.is_set():
        t0 = time.perf_counter()
        try:
            if c.set(f"k{wid}_{local}", f"v{local}"):
                local += 1
            else:
                local_err += 1
        except Exception:
            local_err += 1
        t1 = time.perf_counter()
        local_lat.append((t1 - t0) * 1000)
        if local % 100 == 0:
            with metrics_lock:
                counts[node_idx] += local
                errors[node_idx] += local_err
                latencies[node_idx].extend(local_lat)
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
        t = threading.Thread(target=worker, args=(ni, ni * 1000 + w))
        t.daemon = True
        t.start()
        threads.append(t)

print(f"Stress test: {len(NODES)} nodes × {WORKERS_PER_NODE} workers = {len(NODES)*WORKERS_PER_NODE} threads for {DURATION}s")
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

print(f"\n=== RESULTS ({elapsed:.1f}s) ===")
print(f"Total SET:    {total:,}")
print(f"Errors:       {total_err}")
print(f"TPS:          {total/elapsed:,.0f}")
print(f"Latency:      p50={p50:.3f}ms  p90={p90:.3f}ms  p99={p99:.3f}ms")
print(f"Per-node:     ", end="")
for i in range(len(NODES)):
    print(f"N{i+1}={counts[i]:,}  ", end="")
print()

# Check server CPU load
print("\n=== SERVER STATS ===")
os.system("ps aux | grep antzkv-server | grep -v grep | awk '{print $3, $4, $11}'")
