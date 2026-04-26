package com.antzkv;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TestMultithread {
    static final String HOST = "127.0.0.1";
    static final int PORT = 6379;
    static final int THREADS = 10;
    static final int OPS = 100;

    public static void main(String[] args) throws Exception {
        testSingleClient();
        testPool();
    }

    /** One client shared across threads. */
    static void testSingleClient() throws Exception {
        AntzKVClient client = new AntzKVClient(HOST, PORT);
        client.connect();
        ExecutorService exec = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        List<String> errors = new CopyOnWriteArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            futures.add(exec.submit(() -> {
                for (int i = 0; i < OPS; i++) {
                    String key = "t" + tid + "_k" + i;
                    String val = "v" + tid + "_" + i;
                    try {
                        client.set(key, val);
                        String got = client.get(key);
                        if (!val.equals(got)) {
                            errors.add("GET mismatch " + key + ": expected " + val + ", got " + got);
                        }
                        if (i % 10 == 0) client.delete(key);
                    } catch (Exception e) {
                        errors.add(key + " -> " + e.getMessage());
                    }
                }
            }));
        }
        for (Future<?> f : futures) f.get();
        exec.shutdown();
        client.close();
        System.out.println("[Single Client] Errors: " + errors.size());
        errors.stream().limit(5).forEach(e -> System.out.println("  " + e));
    }

    /** Connection pool test. */
    static void testPool() throws Exception {
        AntzKVClient.Pool pool = new AntzKVClient.Pool(HOST, PORT, 4);
        ExecutorService exec = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        List<String> errors = new CopyOnWriteArrayList<>();
        long start = System.nanoTime();

        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            futures.add(exec.submit(() -> {
                for (int i = 0; i < OPS; i++) {
                    String key = "pool_t" + tid + "_k" + i;
                    String val = "pool_v" + tid + "_" + i;
                    try {
                        AntzKVClient c = pool.acquire();
                        try {
                            c.set(key, val);
                        } finally {
                            pool.release(c);
                        }
                    } catch (Exception e) {
                        errors.add(key + " -> " + e.getMessage());
                    }
                }
            }));
        }
        for (Future<?> f : futures) f.get();
        exec.shutdown();
        double elapsed = (System.nanoTime() - start) / 1e9;
        pool.close();
        int total = THREADS * OPS;
        System.out.printf("[Pool] %d ops in %.2fs = %.0f ops/sec | Errors: %d%n",
                          total, elapsed, total / elapsed, errors.size());
    }
}
