#include "kvdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define NUM_THREADS 8
#define OPS_PER_THREAD 100

void *stress_worker(void *arg) {
    kv_table_t *db = (kv_table_t *)arg;
    unsigned long tid = (unsigned long)pthread_self();

    for (int i = 0; i < OPS_PER_THREAD; ++i) {
        char key[64], val[64];
        snprintf(key, sizeof(key), "key_%lu_%d", tid, i);
        snprintf(val, sizeof(val), "val_%lu_%d", tid, i);
        if (kv_set(db, key, val) != 0) {
            fprintf(stderr, "SET failed: %s\n", key);
        }
        char *got = kv_get(db, key);
        if (!got || strcmp(got, val) != 0) {
            fprintf(stderr, "GET mismatch for %s\n", key);
        }
        free(got);
    }
    return NULL;
}

int main(void) {
    printf("=== antzkv library test ===\n");

    kv_table_t *db = kv_open(NULL);
    if (!db) { fprintf(stderr, "Failed to open DB\n"); return 1; }

    /* Basic CRUD */
    kv_set(db, "foo", "bar");
    char *v = kv_get(db, "foo");
    if (!v || strcmp(v, "bar") != 0) {
        fprintf(stderr, "Basic test FAILED\n");
        return 1;
    }
    free(v);
    printf("[PASS] Basic SET/GET\n");

    /* Concurrent stress */
    pthread_t threads[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; ++i) {
        if (pthread_create(&threads[i], NULL, stress_worker, db) != 0) {
            perror("pthread_create");
            return 1;
        }
    }
    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(threads[i], NULL);
    }
    printf("[PASS] Concurrent stress (%d threads x %d ops)\n", NUM_THREADS, OPS_PER_THREAD);

    /* Persistence */
    kv_table_t *db2 = kv_open("test.db");
    kv_set(db2, "persist", "true");
    kv_close(db2);

    kv_table_t *db3 = kv_open("test.db");
    char *pv = kv_get(db3, "persist");
    if (pv && strcmp(pv, "true") == 0) {
        printf("[PASS] Persistence\n");
    } else {
        fprintf(stderr, "Persistence test FAILED\n");
        return 1;
    }
    free(pv);
    kv_close(db3);
    remove("test.db");

    /* DEL / EXISTS */
    kv_set(db, "to_remove", "x");
    if (!kv_exists(db, "to_remove")) {
        fprintf(stderr, "EXISTS before DEL failed\n"); return 1;
    }
    if (kv_del(db, "to_remove") != 0) {
        fprintf(stderr, "DEL failed\n"); return 1;
    }
    if (kv_exists(db, "to_remove")) {
        fprintf(stderr, "EXISTS after DEL failed\n"); return 1;
    }
    printf("[PASS] DEL / EXISTS\n");

    /* KEYS */
    char **keys = NULL;
    size_t kcount = 0;
    if (kv_keys(db, &keys, &kcount) != 0) {
        fprintf(stderr, "KEYS failed\n"); return 1;
    }
    printf("[PASS] KEYS returned %zu keys\n", kcount);
    kv_keys_free(keys, kcount);

    kv_close(db);
    printf("=== ALL LIBRARY TESTS PASSED ===\n");
    return 0;
}
