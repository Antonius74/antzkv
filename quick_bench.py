#!/usr/bin/env python3
"""Quick pipeline benchmark with hard timeout."""
import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "clients", "python"))
from antzkv import AntzKVClient

PORT = 6301
BATCH = 100
OPS = 5000

c = AntzKVClient("127.0.0.1", PORT)
c.connect()

# Warmup
for i in range(10):
    c.set(f"w{i}", "warmup")
print("Warmup done")

# Sequential pipeline benchmark
t0 = time.perf_counter()
for _ in range(OPS // BATCH):
    pairs = [(f"k_{_}_{i}", f"v{i}") for i in range(BATCH)]
    c.pipeline_set(pairs)
t1 = time.perf_counter()
elapsed = t1 - t0

print(f"Pipeline SET: {OPS} ops in {elapsed:.3f}s = {OPS/elapsed:,.0f} TPS")
print(f"Batch latency: {elapsed/(OPS/BATCH)*1000:.3f}ms per batch")

# Random reads
keys = [f"k_{i}_{0}" for i in range(1000)]
t2 = time.perf_counter()
vals = c.pipeline_get(keys)
t3 = time.perf_counter()
print(f"Pipeline GET: {len(keys)} ops in {t3-t2:.3f}s = {len(keys)/(t3-t2):,.0f} TPS")

c.close()
print("Done.")
