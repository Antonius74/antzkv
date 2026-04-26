import threading
import time
from antzkv import AntzKVClient, AntzKVPool

HOST = "127.0.0.1"
PORT = 6379
NUM_THREADS = 10
OPS = 100

def test_single_client_multithread():
    """One client shared across threads."""
    client = AntzKVClient(HOST, PORT)
    client.connect()
    errors = []
    lock = threading.Lock()

    def worker(tid):
        for i in range(OPS):
            key = f"t{tid}_k{i}"
            val = f"v{tid}_{i}"
            if not client.set(key, val):
                with lock: errors.append(f"SET failed {key}")
            got = client.get(key)
            if got != val:
                with lock: errors.append(f"GET mismatch {key}: expected {val}, got {got}")
            if i % 10 == 0:
                client.delete(key)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(NUM_THREADS)]
    for t in threads: t.start()
    for t in threads: t.join()

    client.close()
    print(f"[Single Client] Errors: {len(errors)}")
    if errors:
        for e in errors[:5]: print("  ", e)

def test_pool():
    """Connection pool with concurrent threads."""
    pool = AntzKVPool(HOST, PORT, size=4)
    errors = []
    lock = threading.Lock()

    def worker(tid):
        for i in range(OPS):
            c = pool.acquire()
            try:
                key = f"pool_t{tid}_k{i}"
                val = f"pool_v{t}_{i}"
                if not c.set(key, val):
                    with lock: errors.append(f"SET failed {key}")
            finally:
                pool.release(c)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(NUM_THREADS)]
    start = time.time()
    for t in threads: t.start()
    for t in threads: t.join()
    elapsed = time.time() - start

    pool.close()
    print(f"[Pool] {NUM_THREADS * OPS} ops in {elapsed:.2f}s = {NUM_THREADS*OPS/elapsed:.0f} ops/sec | Errors: {len(errors)}")

if __name__ == "__main__":
    test_single_client_multithread()
    test_pool()
