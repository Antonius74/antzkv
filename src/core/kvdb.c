#include "kvdb.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

#define INITIAL_CAPACITY 16
#define LOAD_FACTOR 0.75

typedef enum {
    ENTRY_EMPTY = 0,
    ENTRY_OCCUPIED,
    ENTRY_DELETED
} entry_state;

typedef struct {
    char *key;
    char *value;
    entry_state state;
} kv_entry;

struct kv_table {
    kv_entry *entries;
    size_t capacity;
    size_t count;
    char *path;
    pthread_rwlock_t lock;
};

/* ---- FNV-1a hash ---- */
static uint64_t fnv1a(const char *s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 0x100000001b3ULL;
    }
    return h;
}

/* ---- probing quadratico ---- */
static size_t probe(kv_table_t *db, const char *key, int *found) {
    uint64_t h = fnv1a(key) % db->capacity;
    size_t idx = (size_t)h;
    size_t i = 1;
    *found = 0;
    while (db->entries[idx].state != ENTRY_EMPTY) {
        if (db->entries[idx].state == ENTRY_OCCUPIED && strcmp(db->entries[idx].key, key) == 0) {
            *found = 1;
            return idx;
        }
        idx = (idx + i * i) % db->capacity;
        ++i;
        if (i > db->capacity) break;
    }
    return idx; /* primo slot libero */
}

/* ---- espansione tabella ---- */
static int resize(kv_table_t *db) {
    size_t old_cap = db->capacity;
    kv_entry *old = db->entries;

    db->capacity *= 2;
    db->entries = calloc(db->capacity, sizeof(kv_entry));
    if (!db->entries) {
        db->entries = old;
        db->capacity = old_cap;
        return -1;
    }
    db->count = 0;

    for (size_t i = 0; i < old_cap; ++i) {
        if (old[i].state == ENTRY_OCCUPIED) {
            int found;
            size_t idx = probe(db, old[i].key, &found);
            db->entries[idx] = old[i];
            db->entries[idx].state = ENTRY_OCCUPIED;
            ++db->count;
        }
    }
    free(old);
    return 0;
}

/* ---- Creazione / caricamento ---- */
kv_table_t *kv_open(const char *path) {
    kv_table_t *db = malloc(sizeof(*db));
    if (!db) return NULL;
    db->entries = calloc(INITIAL_CAPACITY, sizeof(kv_entry));
    if (!db->entries) { free(db); return NULL; }
    db->capacity = INITIAL_CAPACITY;
    db->count = 0;
    db->path = path ? strdup(path) : NULL;
    pthread_rwlock_init(&db->lock, NULL);

    if (db->path) {
        FILE *fp = fopen(db->path, "r");
        if (fp) {
            size_t n;
            if (fread(&n, sizeof(n), 1, fp) == 1) {
                for (size_t i = 0; i < n; ++i) {
                    size_t kl, vl;
                    char *k = NULL, *v = NULL;
                    if (fread(&kl, sizeof(kl), 1, fp) != 1) break;
                    k = malloc(kl + 1);
                    if (fread(k, 1, kl, fp) != kl) { free(k); break; }
                    k[kl] = '\0';
                    if (fread(&vl, sizeof(vl), 1, fp) != 1) { free(k); break; }
                    v = malloc(vl + 1);
                    if (fread(v, 1, vl, fp) != vl) { free(k); free(v); break; }
                    v[vl] = '\0';
                    kv_set(db, k, v);
                    free(k); free(v);
                }
            }
            fclose(fp);
        }
    }
    return db;
}

void kv_close(kv_table_t *db) {
    if (!db) return;
    kv_save(db);
    for (size_t i = 0; i < db->capacity; ++i) {
        if (db->entries[i].state == ENTRY_OCCUPIED) {
            free(db->entries[i].key);
            free(db->entries[i].value);
        }
    }
    free(db->entries);
    free(db->path);
    pthread_rwlock_destroy(&db->lock);
    free(db);
}

/* ---- SET ---- */
int kv_set(kv_table_t *db, const char *key, const char *value) {
    if (!db || !key || !value) return -1;
    pthread_rwlock_wrlock(&db->lock);
    if ((double)(db->count + 1) / db->capacity > LOAD_FACTOR) {
        if (resize(db) != 0) {
            pthread_rwlock_unlock(&db->lock);
            return -1;
        }
    }
    int found;
    size_t idx = probe(db, key, &found);
    if (found) {
        free(db->entries[idx].value);
        db->entries[idx].value = strdup(value);
    } else {
        db->entries[idx].key = strdup(key);
        db->entries[idx].value = strdup(value);
        db->entries[idx].state = ENTRY_OCCUPIED;
        ++db->count;
    }
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

/* ---- GET ---- */
char *kv_get(kv_table_t *db, const char *key) {
    if (!db || !key) return NULL;
    pthread_rwlock_rdlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    char *res = NULL;
    if (found) res = strdup(db->entries[idx].value);
    pthread_rwlock_unlock(&db->lock);
    return res;
}

/* ---- DEL ---- */
int kv_del(kv_table_t *db, const char *key) {
    if (!db || !key) return -1;
    pthread_rwlock_wrlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    if (found) {
        free(db->entries[idx].key);
        free(db->entries[idx].value);
        db->entries[idx].state = ENTRY_DELETED;
        --db->count;
    }
    pthread_rwlock_unlock(&db->lock);
    return found ? 0 : -1;
}

/* ---- EXISTS ---- */
int kv_exists(kv_table_t *db, const char *key) {
    if (!db || !key) return 0;
    pthread_rwlock_rdlock(&db->lock);
    int found;
    probe(db, key, &found);
    pthread_rwlock_unlock(&db->lock);
    return found;
}

/* ---- SAVE ---- */
int kv_save(kv_table_t *db) {
    if (!db || !db->path) return -1;
    pthread_rwlock_rdlock(&db->lock);
    FILE *fp = fopen(db->path, "w");
    if (!fp) { pthread_rwlock_unlock(&db->lock); return -1; }
    fwrite(&db->count, sizeof(db->count), 1, fp);
    for (size_t i = 0; i < db->capacity; ++i) {
        if (db->entries[i].state == ENTRY_OCCUPIED) {
            size_t kl = strlen(db->entries[i].key);
            size_t vl = strlen(db->entries[i].value);
            fwrite(&kl, sizeof(kl), 1, fp);
            fwrite(db->entries[i].key, 1, kl, fp);
            fwrite(&vl, sizeof(vl), 1, fp);
            fwrite(db->entries[i].value, 1, vl, fp);
        }
    }
    fclose(fp);
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

/* ---- KEYS ---- */
int kv_keys(kv_table_t *db, char ***out_keys, size_t *out_count) {
    if (!db || !out_keys || !out_count) return -1;
    pthread_rwlock_rdlock(&db->lock);
    size_t n = db->count;
    *out_keys = malloc(n * sizeof(char *));
    if (!*out_keys) { pthread_rwlock_unlock(&db->lock); return -1; }
    size_t j = 0;
    for (size_t i = 0; i < db->capacity && j < n; ++i) {
        if (db->entries[i].state == ENTRY_OCCUPIED) {
            (*out_keys)[j] = strdup(db->entries[i].key);
            if (!(*out_keys)[j]) {
                for (size_t k = 0; k < j; ++k) free((*out_keys)[k]);
                free(*out_keys);
                pthread_rwlock_unlock(&db->lock);
                return -1;
            }
            ++j;
        }
    }
    *out_count = j;
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

void kv_keys_free(char **keys, size_t count) {
    for (size_t i = 0; i < count; ++i) free(keys[i]);
    free(keys);
}
