#include "kvdb.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <math.h>

#define INITIAL_CAPACITY  32
#define LOAD_FACTOR       0.75

static int64_t now_msec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000LL + (int64_t)tv.tv_usec / 1000LL;
}

static uint64_t now_usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
}

/* ------------------------------------------------------------------ */
/*  Linked-list node (for List objects)                                */
/* ------------------------------------------------------------------ */
typedef struct list_node {
    char              *value;
    struct list_node  *prev, *next;
} list_node_t;

typedef struct dlist {
    list_node_t *head, *tail;
    size_t       len;
} dlist_t;

/* ------------------------------------------------------------------ */
/*  Hash-table node (for Set / Hash objects – closed addressing)       */
/* ------------------------------------------------------------------ */
typedef struct sht_node {
    char             *key;            /* member / field                 */
    char             *value;          /* NULL for Set, value for Hash   */
    struct sht_node  *next;
} sht_node_t;

typedef struct simple_ht {
    sht_node_t **buckets;
    size_t       cap;
    size_t       count;
} simple_ht_t;

/* ------------------------------------------------------------------ */
/*  Skip-list (Sorted Set)                                             */
/* ------------------------------------------------------------------ */
#define ZSKIPLIST_MAXLEVEL 12

typedef struct zskiplist_node {
    char    *member;
    double   score;
    struct zskiplist_node **forward;
    struct zskiplist_node  *backward;
    unsigned                level;
} zskiplist_node_t;

typedef struct zskiplist {
    zskiplist_node_t *header, *tail;
    size_t   length;
    unsigned level;
    zskiplist_node_t **update_buf;   /* temp for insert/delete */
    unsigned *rank_buf;              /* temp */
} zskiplist_t;

/* ------------------------------------------------------------------ */
/*  Pub-Sub manager                                                    */
/* ------------------------------------------------------------------ */
typedef struct client_sub {
    int                 fd;
    pthread_mutex_t    *wlock;
    struct client_sub  *next;
} client_sub_t;

typedef struct channel_entry {
    char    name[CHANNEL_NAME_MAX];
    client_sub_t *subs;
    struct  channel_entry *next;
} channel_entry_t;

struct pubsub_mgr {
    channel_entry_t *channels;
    pthread_rwlock_t lock;
};

/* ================================================================== */
/*  Forward declarations for static helpers                            */
/* ================================================================== */
static void    dlist_push_left(dlist_t *lst, const char *val);
static void    dlist_push_right(dlist_t *lst, const char *val);
static char   *dlist_pop_left(dlist_t *lst);
static char   *dlist_pop_right(dlist_t *lst);
static char   *dlist_get(dlist_t *lst, int64_t idx);
static int     dlist_set(dlist_t *lst, int64_t idx, const char *val);
static int     dlist_rem(dlist_t *lst, int64_t count, const char *val);
static void    dlist_free(dlist_t *lst);

static simple_ht_t *ht_create(void);
static int      ht_add(simple_ht_t *ht, const char *key, const char *val,
                       int replace);
static int      ht_remove(simple_ht_t *ht, const char *key);
static int      ht_exists(simple_ht_t *ht, const char *key);
static char    *ht_get(simple_ht_t *ht, const char *key);
static void     ht_keys(simple_ht_t *ht, char ***out, size_t *count);
static void     ht_items(simple_ht_t *ht, char ***out, size_t *count);
static void     ht_free(simple_ht_t *ht);

static zskiplist_t *zsl_create(void);
static zskiplist_node_t *zsl_insert(zskiplist_t *zsl, double score,
                                    const char *member);
static int   zsl_delete(zskiplist_t *zsl, double score, const char *member);
static int   zsl_rank(const zskiplist_t *zsl, double score,
                      const char *member);
static int   zsl_revrank(const zskiplist_t *zsl, double score,
                         const char *member);
static void  zsl_free(zskiplist_t *zsl);

static uint64_t fnv1a(const char *s);

/* ================================================================== */
/*  Object constructors / destructors                                  */
/* ================================================================== */
db_object_t *obj_create_string(const char *s) {
    db_object_t *o = calloc(1, sizeof(*o));
    if (!o) return NULL;
    o->type = OBJ_STRING;
    o->ptr  = strdup(s ? s : "");
    o->ttl  = -1;
    o->wallclock = now_usec();
    return o;
}

db_object_t *obj_create_list(void) {
    db_object_t *o = calloc(1, sizeof(*o));
    if (!o) return NULL;
    dlist_t *lst = calloc(1, sizeof(*lst));
    if (!lst) { free(o); return NULL; }
    o->type = OBJ_LIST;
    o->ptr  = lst;
    o->ttl  = -1;
    return o;
}

db_object_t *obj_create_set(void) {
    db_object_t *o = calloc(1, sizeof(*o));
    if (!o) return NULL;
    simple_ht_t *ht = ht_create();
    if (!ht) { free(o); return NULL; }
    o->type = OBJ_SET;
    o->ptr  = ht;
    o->ttl  = -1;
    return o;
}

db_object_t *obj_create_hash(void) {
    db_object_t *o = calloc(1, sizeof(*o));
    if (!o) return NULL;
    simple_ht_t *ht = ht_create();
    if (!ht) { free(o); return NULL; }
    o->type = OBJ_HASH;
    o->ptr  = ht;
    o->ttl  = -1;
    return o;
}

db_object_t *obj_create_zset(void) {
    db_object_t *o = calloc(1, sizeof(*o));
    if (!o) return NULL;
    zskiplist_t *zsl = zsl_create();
    if (!zsl) { free(o); return NULL; }
    o->type = OBJ_ZSET;
    o->ptr  = zsl;
    o->ttl  = -1;
    return o;
}

void obj_free(db_object_t *o) {
    if (!o) return;
    switch (o->type) {
    case OBJ_STRING: free(o->ptr); break;
    case OBJ_LIST:   dlist_free((dlist_t*)o->ptr); break;
    case OBJ_SET:
    case OBJ_HASH:   ht_free((simple_ht_t*)o->ptr); break;
    case OBJ_ZSET:   zsl_free((zskiplist_t*)o->ptr); break;
    }
    free(o);
}

/* Return a malloc'd human-readable representation */
char *obj_to_string(const db_object_t *o) {
    if (!o) return strdup("(nil)");
    switch (o->type) {
    case OBJ_STRING: return strdup((char*)o->ptr);
    case OBJ_LIST: {
        dlist_t *l = (dlist_t*)o->ptr;
        char buf[32];
        snprintf(buf, sizeof(buf), "(list len=%zu)", l->len);
        return strdup(buf);
    }
    case OBJ_SET: {
        simple_ht_t *h = (simple_ht_t*)o->ptr;
        char buf[32];
        snprintf(buf, sizeof(buf), "(set card=%zu)", h->count);
        return strdup(buf);
    }
    case OBJ_HASH: {
        simple_ht_t *h = (simple_ht_t*)o->ptr;
        char buf[32];
        snprintf(buf, sizeof(buf), "(hash len=%zu)", h->count);
        return strdup(buf);
    }
    case OBJ_ZSET: {
        zskiplist_t *z = (zskiplist_t*)o->ptr;
        char buf[48];
        snprintf(buf, sizeof(buf), "(zset card=%zu)", z->length);
        return strdup(buf);
    }
    }
    return strdup("(unknown)");
}

/* ================================================================== */
/*  Hash-table (key-space) entry                                       */
/* ================================================================== */
typedef enum { E_EMPTY=0, E_OCCUPIED, E_DELETED } entry_state_t;

typedef struct {
    char          *key;
    db_object_t   *obj;
    entry_state_t  state;
} kv_entry;

struct kv_table {
    kv_entry       *entries;
    size_t          capacity;
    size_t          count;
    char           *path;
    pthread_rwlock_t lock;
    uint64_t         db_version;
    pthread_mutex_t  version_lock;
};

static uint64_t fnv1a(const char *s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    while (*s) {
        h ^= (unsigned char)*s++;
        h *= 0x100000001b3ULL;
    }
    return h;
}

static size_t probe(kv_table_t *db, const char *key, int *found) {
    uint64_t h = fnv1a(key) % db->capacity;
    size_t idx = (size_t)h;
    size_t i = 1;
    *found = 0;
    while (db->entries[idx].state != E_EMPTY) {
        if (db->entries[idx].state == E_OCCUPIED &&
            strcmp(db->entries[idx].key, key) == 0) {
            *found = 1;
            return idx;
        }
        idx = (idx + i * i) % db->capacity;
        ++i;
        if (i > db->capacity) break;
    }
    return idx;
}

static int resize(kv_table_t *db) {
    size_t old_cap = db->capacity;
    kv_entry *old  = db->entries;
    db->capacity  *= 2;
    db->entries    = calloc(db->capacity, sizeof(kv_entry));
    if (!db->entries) {
        db->entries  = old;
        db->capacity = old_cap;
        return -1;
    }
    db->count = 0;
    for (size_t i = 0; i < old_cap; ++i) {
        if (old[i].state == E_OCCUPIED) {
            int found;
            size_t idx = probe(db, old[i].key, &found);
            db->entries[idx] = old[i];
            db->entries[idx].state = E_OCCUPIED;
            ++db->count;
        }
    }
    free(old);
    return 0;
}

/* LWW comparator: 1 if a > b */
static int meta_gt(const kv_meta_t *a, const kv_meta_t *b) {
    if (a->version   != b->version)   return a->version   > b->version;
    if (a->wallclock != b->wallclock) return a->wallclock > b->wallclock;
    return strcmp(a->origin, b->origin) > 0;
}

/* ---------- versioning ---------- */
uint64_t kv_next_version(kv_table_t *db) {
    pthread_mutex_lock(&db->version_lock);
    uint64_t v = ++db->db_version;
    pthread_mutex_unlock(&db->version_lock);
    return v;
}

uint64_t kv_db_version(kv_table_t *db) {
    pthread_mutex_lock(&db->version_lock);
    uint64_t v = db->db_version;
    pthread_mutex_unlock(&db->version_lock);
    return v;
}

/* ---------- open / close ---------- */
kv_table_t *kv_open(const char *path) {
    kv_table_t *db = calloc(1, sizeof(*db));
    if (!db) return NULL;
    db->entries = calloc(INITIAL_CAPACITY, sizeof(kv_entry));
    if (!db->entries) { free(db); return NULL; }
    db->capacity = INITIAL_CAPACITY;
    db->count = 0;
    db->path   = path ? strdup(path) : NULL;
    pthread_rwlock_init(&db->lock, NULL);
    pthread_mutex_init(&db->version_lock, NULL);
    db->db_version = 0;

    if (db->path) {
        FILE *fp = fopen(db->path, "r");
        if (fp) {
            size_t n;
            if (fread(&n, sizeof(n), 1, fp) == 1) {
                for (size_t i = 0; i < n; ++i) {
                    size_t kl, vl;
                    uint64_t ver, wc;
                    char origin[16] = {0};
                    char *k = NULL, *v = NULL;
                    if (fread(&kl, sizeof(kl), 1, fp) != 1) break;
                    k = malloc(kl + 1);
                    if (!k || fread(k,1,kl,fp) != kl) { free(k); break; }
                    k[kl] = '\0';
                    if (fread(&vl, sizeof(vl), 1, fp) != 1) { free(k); break; }
                    v = malloc(vl + 1);
                    if (!v || fread(v,1,vl,fp) != vl) { free(k);free(v);break;}
                    v[vl] = '\0';
                    if (fread(&ver,sizeof(ver),1,fp)!=1) {free(k);free(v);break;}
                    if (fread(&wc,sizeof(wc),1,fp)!=1) {free(k);free(v);break;}
                    if (fread(origin,1,15,fp)!=15) {free(k);free(v);break;}
                    origin[15]='\0';
                    kv_meta_t m = {ver,wc,""};
                    memcpy(m.origin,origin,15);
                    kv_set_meta(db,k,v,&m);
                    free(k);free(v);
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
        if (db->entries[i].state == E_OCCUPIED) {
            free(db->entries[i].key);
            obj_free(db->entries[i].obj);
        }
    }
    free(db->entries);
    free(db->path);
    pthread_rwlock_destroy(&db->lock);
    pthread_mutex_destroy(&db->version_lock);
    free(db);
}

/* ---------- generic SET (object) ---------- */
int kv_set_object(kv_table_t *db, const char *key, db_object_t *obj,
                  const kv_meta_t *meta) {
    if (!db || !key || !obj) return -1;
    pthread_rwlock_wrlock(&db->lock);
    if ((double)(db->count + 1) / db->capacity > LOAD_FACTOR) {
        if (resize(db) != 0) { pthread_rwlock_unlock(&db->lock); return -1; }
    }
    int found;
    size_t idx = probe(db, key, &found);
    if (found) {
        obj_free(db->entries[idx].obj);
        db->entries[idx].obj = obj;
        if (meta) {
            obj->version   = meta->version;
            obj->wallclock = meta->wallclock;
            memcpy(obj->origin, meta->origin, 15);
            obj->origin[15] = '\0';
        }
    } else {
        db->entries[idx].key   = strdup(key);
        db->entries[idx].obj   = obj;
        db->entries[idx].state = E_OCCUPIED;
        if (meta) {
            obj->version   = meta->version;
            obj->wallclock = meta->wallclock;
            memcpy(obj->origin, meta->origin, 15);
            obj->origin[15] = '\0';
        }
        ++db->count;
    }
    if (meta) {
        pthread_mutex_lock(&db->version_lock);
        if (meta->version > db->db_version) db->db_version = meta->version;
        pthread_mutex_unlock(&db->version_lock);
    }
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

/* ---------- generic GET ---------- */
db_object_t *kv_get_object(kv_table_t *db, const char *key) {
    if (!db || !key) return NULL;
    pthread_rwlock_rdlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    db_object_t *obj = found ? db->entries[idx].obj : NULL;
    pthread_rwlock_unlock(&db->lock);
    return obj;
}

/* ---------- DEL ---------- */
int kv_del(kv_table_t *db, const char *key) {
    if (!db || !key) return -1;
    pthread_rwlock_wrlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    if (!found) { pthread_rwlock_unlock(&db->lock); return -1; }
    free(db->entries[idx].key);
    obj_free(db->entries[idx].obj);
    db->entries[idx].state = E_DELETED;
    --db->count;
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

int kv_del_meta(kv_table_t *db, const char *key, const kv_meta_t *meta) {
    if (!db || !key || !meta) return -1;
    pthread_rwlock_wrlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    if (found) {
        db_object_t *obj = db->entries[idx].obj;
        kv_meta_t cur = {obj->version, obj->wallclock, ""};
        memcpy(cur.origin, obj->origin, 15); cur.origin[15] = '\0';
        if (meta_gt(&cur, meta)) {
            pthread_rwlock_unlock(&db->lock);
            return 0;
        }
        free(db->entries[idx].key);
        obj_free(obj);
        db->entries[idx].state = E_DELETED;
        --db->count;
    }
    pthread_mutex_lock(&db->version_lock);
    if (meta->version > db->db_version) db->db_version = meta->version;
    pthread_mutex_unlock(&db->version_lock);
    pthread_rwlock_unlock(&db->lock);
    return found ? 0 : -1;
}

/* ---------- typed helpers (String) ---------- */
int kv_set(kv_table_t *db, const char *key, const char *value) {
    uint64_t ver = kv_next_version(db);
    kv_meta_t m = {ver, now_usec(), ""};
    return kv_set_meta(db, key, value, &m);
}

int kv_set_meta(kv_table_t *db, const char *key, const char *value,
                const kv_meta_t *meta) {
    db_object_t *obj = obj_create_string(value);
    if (!obj) return -1;
    obj->version   = meta->version;
    obj->wallclock = meta->wallclock;
    memcpy(obj->origin, meta->origin, 15);
    obj->origin[15] = '\0';
    return kv_set_object(db, key, obj, meta);
}

char *kv_get(kv_table_t *db, const char *key) {
    db_object_t *obj = kv_get_object(db, key);
    if (!obj) return NULL;
    if (obj->type != OBJ_STRING) return NULL;
    return strdup((char*)obj->ptr);
}

char *kv_get_meta(kv_table_t *db, const char *key, kv_meta_t *out) {
    if (!db || !key) return NULL;
    pthread_rwlock_rdlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    char *res = NULL;
    if (found && db->entries[idx].obj->type == OBJ_STRING) {
        res = strdup((char*)db->entries[idx].obj->ptr);
        if (out) {
            out->version   = db->entries[idx].obj->version;
            out->wallclock = db->entries[idx].obj->wallclock;
            memcpy(out->origin, db->entries[idx].obj->origin, 15);
            out->origin[15] = '\0';
        }
    }
    pthread_rwlock_unlock(&db->lock);
    return res;
}

int kv_exists(kv_table_t *db, const char *key) {
    if (!db || !key) return 0;
    pthread_rwlock_rdlock(&db->lock);
    int found;
    probe(db, key, &found);
    pthread_rwlock_unlock(&db->lock);
    return found;
}

int kv_keys(kv_table_t *db, char ***out_keys, size_t *out_count) {
    if (!db || !out_keys || !out_count) return -1;
    pthread_rwlock_rdlock(&db->lock);
    size_t n = db->count;
    *out_keys = malloc(n * sizeof(char*));
    if (!*out_keys) { pthread_rwlock_unlock(&db->lock); return -1; }
    size_t j = 0;
    for (size_t i = 0; i < db->capacity && j < n; ++i) {
        if (db->entries[i].state == E_OCCUPIED) {
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

void kv_free_array(char **arr, size_t count) {
    kv_keys_free(arr, count);
}

/* ---------- SAVE ---------- */
int kv_save(kv_table_t *db) {
    if (!db || !db->path) return -1;
    pthread_rwlock_rdlock(&db->lock);
    FILE *fp = fopen(db->path, "w");
    if (!fp) { pthread_rwlock_unlock(&db->lock); return -1; }
    fwrite(&db->count, sizeof(db->count), 1, fp);
    for (size_t i = 0; i < db->capacity; ++i) {
        if (db->entries[i].state == E_OCCUPIED &&
            db->entries[i].obj->type == OBJ_STRING) {
            char *k = db->entries[i].key;
            char *v = (char*)db->entries[i].obj->ptr;
            size_t kl = strlen(k), vl = strlen(v);
            fwrite(&kl, sizeof(kl), 1, fp); fwrite(k, 1, kl, fp);
            fwrite(&vl, sizeof(vl), 1, fp); fwrite(v, 1, vl, fp);
            uint64_t ver = db->entries[i].obj->version;
            uint64_t wc  = db->entries[i].obj->wallclock;
            char orig[16] = {0};
            memcpy(orig, db->entries[i].obj->origin, 15);
            fwrite(&ver, sizeof(ver), 1, fp);
            fwrite(&wc,  sizeof(wc),  1, fp);
            fwrite(orig, 1, 15, fp);
        }
    }
    fclose(fp);
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

/* ================================================================== */
/*  TTL                                                                  */
/* ================================================================== */
int64_t kv_ttl_ms(kv_table_t *db, const char *key) {
    if (!db || !key) return -2;
    pthread_rwlock_rdlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    int64_t ttl = -2;         /* key doesn't exist */
    if (found) {
        int64_t t = db->entries[idx].obj->ttl;
        ttl = (t < 0) ? -1 : t;
    }
    pthread_rwlock_unlock(&db->lock);
    return ttl;
}

int kv_set_ttl(kv_table_t *db, const char *key, int64_t ttl) {
    if (!db || !key) return -1;
    pthread_rwlock_wrlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    if (!found) { pthread_rwlock_unlock(&db->lock); return -1; }
    db->entries[idx].obj->ttl = ttl;
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

int kv_expire(kv_table_t *db, const char *key, int64_t msec) {
    return kv_set_ttl(db, key, now_msec() + msec);
}

int kv_persist(kv_table_t *db, const char *key) {
    return kv_set_ttl(db, key, -1);
}

size_t kv_active_expire(kv_table_t *db, size_t max_samples, int64_t now) {
    size_t expired = 0;
    for (size_t s = 0; s < max_samples && s < db->capacity; ++s) {
        pthread_rwlock_wrlock(&db->lock);
        /* random-ish walk */
        static unsigned int seed = 0;
        seed ^= (unsigned int)(uintptr_t)&now;
        size_t idx = (size_t)(rand_r(&seed) % (db->capacity));
        if (db->entries[idx].state == E_OCCUPIED) {
            int64_t ttl = db->entries[idx].obj->ttl;
            if (ttl >= 0 && now >= ttl) {
                free(db->entries[idx].key);
                obj_free(db->entries[idx].obj);
                db->entries[idx].state = E_DELETED;
                --db->count;
                ++expired;
            }
        }
        pthread_rwlock_unlock(&db->lock);
        if (expired >= max_samples / 4) break;
    }
    return expired;
}

/* ================================================================== */
/*  String ops                                                         */
/* ================================================================== */
static db_object_t *get_string_obj(kv_table_t *db, const char *key) {
    pthread_rwlock_rdlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    db_object_t *obj = NULL;
    if (found && db->entries[idx].obj->type == OBJ_STRING)
        obj = db->entries[idx].obj;
    pthread_rwlock_unlock(&db->lock);
    return obj;
}

int kv_incr(kv_table_t *db, const char *key, int64_t *out) {
    return kv_incrby(db, key, 1, out);
}

int kv_incrby(kv_table_t *db, const char *key, int64_t delta, int64_t *out) {
    pthread_rwlock_wrlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    if (!found) {
        pthread_rwlock_unlock(&db->lock);
        db_object_t *obj = obj_create_string("0");
        obj->version = kv_next_version(db);
        kv_set_object(db, key, obj, NULL);
        pthread_rwlock_wrlock(&db->lock);
        probe(db, key, &found);
        idx = probe(db, key, &found);
    }
    db_object_t *obj = db->entries[idx].obj;
    if (obj->type != OBJ_STRING) { pthread_rwlock_unlock(&db->lock); return -1; }
    long long v = 0;
    sscanf((char*)obj->ptr, "%lld", &v);
    v += delta;
    free(obj->ptr);
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", v);
    obj->ptr = strdup(buf);
    *out = v;
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

int kv_decr(kv_table_t *db, const char *key, int64_t *out) {
    return kv_decrby(db, key, 1, out);
}

int kv_decrby(kv_table_t *db, const char *key, int64_t delta, int64_t *out) {
    return kv_incrby(db, key, -delta, out);
}

int kv_append(kv_table_t *db, const char *key, const char *suffix,
              size_t *out_len) {
    db_object_t *obj = get_string_obj(db, key);
    if (!obj) {
        obj = obj_create_string("");
        if (kv_set_object(db, key, obj, NULL) != 0) return -1;
        obj = get_string_obj(db, key);
    }
    pthread_rwlock_wrlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    if (!found || db->entries[idx].obj->type != OBJ_STRING) {
        pthread_rwlock_unlock(&db->lock); return -1;
    }
    char *old = (char*)db->entries[idx].obj->ptr;
    size_t oldl = strlen(old), sul = strlen(suffix);
    char *n = malloc(oldl + sul + 1);
    memcpy(n, old, oldl); memcpy(n + oldl, suffix, sul);
    n[oldl + sul] = '\0';
    free(old);
    db->entries[idx].obj->ptr = n;
    *out_len = oldl + sul;
    pthread_rwlock_unlock(&db->lock);
    return 0;
}

char *kv_getrange(kv_table_t *db, const char *key, int64_t start,
                  int64_t end) {
    db_object_t *obj = get_string_obj(db, key);
    if (!obj) return strdup("");
    const char *s = (char*)obj->ptr;
    int64_t len = (int64_t)strlen(s);
    if (start < 0) start += len;
    if (start < 0) start = 0;
    if (start > len) start = len;
    if (end < 0) end += len;
    if (end < 0) end = -1;
    if (end > len - 1) end = len - 1;
    if (start > end) return strdup("");
    int64_t n = end - start + 1;
    char *res = malloc(n + 1);
    memcpy(res, s + start, n);
    res[n] = '\0';
    return res;
}

int kv_strlen(kv_table_t *db, const char *key, size_t *out) {
    db_object_t *obj = get_string_obj(db, key);
    if (!obj) return -1;
    *out = strlen((char*)obj->ptr);
    return 0;
}

int kv_setnx(kv_table_t *db, const char *key, const char *value) {
    if (kv_exists(db, key)) return 0;
    return kv_set(db, key, value);
}

int kv_setex(kv_table_t *db, const char *key, const char *value,
             int64_t msec) {
    int ret = kv_set(db, key, value);
    if (ret != 0) return ret;
    return kv_set_ttl(db, key, now_msec() + msec);
}

/* ================================================================== */
/*  List commands                                                      */
/* ================================================================== */

/* Helper to get writable object by key. Returns NULL on error/type mismatch */
static db_object_t *wr_obj(kv_table_t *db, const char *key, int type) {
    pthread_rwlock_rdlock(&db->lock);
    int found;
    size_t idx = probe(db, key, &found);
    db_object_t *obj = found ? db->entries[idx].obj : NULL;
    pthread_rwlock_unlock(&db->lock);
    if (!obj) return NULL;
    if (obj->type != type) return NULL;
    return obj;
}

static db_object_t *wr_set_obj(kv_table_t *db, const char *key) {
    db_object_t *o = wr_obj(db, key, OBJ_SET);
    if (!o) {
        o = obj_create_set();
        if (kv_set_object(db, key, o, NULL) != 0) { obj_free(o); return NULL; }
        o = kv_get_object(db, key);
    }
    return o;
}

static int kv_ensure_list(kv_table_t *db, const char *key,
                          db_object_t **out) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    if (!o) {
        o = obj_create_list();
        if (!o) return -1;
        if (kv_set_object(db, key, o, NULL) != 0) {
            obj_free(o); return -1;
        }
        o = wr_obj(db, key, OBJ_LIST);
    }
    *out = o;
    return o ? 0 : -1;
}

int kv_lpush(kv_table_t *db, const char *key, const char *value,
             size_t *out_len) {
    db_object_t *o;
    if (kv_ensure_list(db, key, &o) != 0) return -1;
    dlist_t *l = (dlist_t*)o->ptr;
    dlist_push_left(l, value);
    if (out_len) *out_len = l->len;
    return 0;
}

int kv_rpush(kv_table_t *db, const char *key, const char *value,
             size_t *out_len) {
    db_object_t *o;
    if (kv_ensure_list(db, key, &o) != 0) return -1;
    dlist_t *l = (dlist_t*)o->ptr;
    dlist_push_right(l, value);
    if (out_len) *out_len = l->len;
    return 0;
}

char *kv_lpop(kv_table_t *db, const char *key) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    return o ? dlist_pop_left((dlist_t*)o->ptr) : NULL;
}

char *kv_rpop(kv_table_t *db, const char *key) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    return o ? dlist_pop_right((dlist_t*)o->ptr) : NULL;
}

int kv_llen(kv_table_t *db, const char *key, size_t *out) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    if (!o) { *out = 0; return 0; }
    *out = ((dlist_t*)o->ptr)->len;
    return 0;
}

char *kv_lindex(kv_table_t *db, const char *key, int64_t idx) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    if (!o) return NULL;
    return dlist_get((dlist_t*)o->ptr, idx);
}

int kv_lset(kv_table_t *db, const char *key, int64_t idx,
            const char *value) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    return o ? dlist_set((dlist_t*)o->ptr, idx, value) : -1;
}

int kv_lrem(kv_table_t *db, const char *key, int64_t count,
            const char *value, size_t *removed) {
    db_object_t *o = wr_obj(db, key, OBJ_LIST);
    if (!o) { *removed = 0; return 0; }
    int r = dlist_rem((dlist_t*)o->ptr, count, value);
    *removed = (size_t)r;
    return 0;
}

char *kv_lpoprpush(kv_table_t *db, const char *src, const char *dst) {
    char *v = kv_rpop(db, src);
    if (v) kv_lpush(db, dst, v, NULL);
    return v;
}

/* ================================================================== */
/*  Set commands                                                       */
/* ================================================================== */
int kv_sadd(kv_table_t *db, const char *key,
            const char **members, int member_count, int *added) {
    int a = 0;
    db_object_t *o = wr_set_obj(db, key);
    if (!o) return -1;
    simple_ht_t *ht = (simple_ht_t*)o->ptr;
    for (int i = 0; i < member_count; ++i) {
        if (ht_add(ht, members[i], NULL, 0 /* no replace */) == 1) ++a;
    }
    if (added) *added = a;
    return 0;
}

int kv_srem(kv_table_t *db, const char *key,
            const char **members, int member_count, int *removed) {
    int r = 0;
    db_object_t *o = wr_obj(db, key, OBJ_SET);
    if (!o) { if (removed) *removed = 0; return 0; }
    simple_ht_t *ht = (simple_ht_t*)o->ptr;
    for (int i = 0; i < member_count; ++i)
        if (ht_remove(ht, members[i]) == 1) ++r;
    if (removed) *removed = r;
    return 0;
}

int kv_sismember(kv_table_t *db, const char *key, const char *member) {
    db_object_t *o = wr_obj(db, key, OBJ_SET);
    if (!o) return 0;
    return ht_exists((simple_ht_t*)o->ptr, member);
}

int kv_scard(kv_table_t *db, const char *key, size_t *out) {
    db_object_t *o = wr_obj(db, key, OBJ_SET);
    *out = o ? ((simple_ht_t*)o->ptr)->count : 0;
    return 0;
}

int kv_smembers(kv_table_t *db, const char *key,
                char ***out, size_t *count) {
    db_object_t *o = wr_obj(db, key, OBJ_SET);
    if (!o) { *count = 0; *out = NULL; return 0; }
    ht_keys((simple_ht_t*)o->ptr, out, count);
    return 0;
}

/* ================================================================== */
/*  Hash commands                                                      */
/* ================================================================== */
static db_object_t *wr_hash(kv_table_t *db, const char *key) {
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    if (!o) {
        o = obj_create_hash();
        if (kv_set_object(db, key, o, NULL) != 0) { obj_free(o); return NULL; }
        o = kv_get_object(db, key);
    }
    return o;
}

int kv_hset(kv_table_t *db, const char *key, const char *field,
            const char *value, int *created) {
    db_object_t *o = wr_hash(db, key);
    if (!o) return -1;
    simple_ht_t *ht = (simple_ht_t*)o->ptr;
    int existed = ht_exists(ht, field);
    ht_add(ht, field, value, 1 /* replace */);
    if (created) *created = existed ? 0 : 1;
    return 0;
}

char *kv_hget(kv_table_t *db, const char *key, const char *field) {
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    if (!o) return NULL;
    char *v = ht_get((simple_ht_t*)o->ptr, field);
    return v ? strdup(v) : NULL;
}

int kv_hdel(kv_table_t *db, const char *key,
            const char **fields, int field_count, int *removed) {
    int r = 0;
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    if (!o) { if (removed) *removed = 0; return 0; }
    simple_ht_t *ht = (simple_ht_t*)o->ptr;
    for (int i = 0; i < field_count; ++i)
        if (ht_remove(ht, fields[i]) == 1) ++r;
    if (removed) *removed = r;
    return 0;
}

int kv_hexists(kv_table_t *db, const char *key, const char *field) {
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    return o ? ht_exists((simple_ht_t*)o->ptr, field) : 0;
}

int kv_hlen(kv_table_t *db, const char *key, size_t *out) {
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    *out = o ? ((simple_ht_t*)o->ptr)->count : 0;
    return 0;
}

int kv_hkeys(kv_table_t *db, const char *key, char ***out, size_t *count) {
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    if (!o) { *count = 0; *out = NULL; return 0; }
    ht_keys((simple_ht_t*)o->ptr, out, count);
    return 0;
}

int kv_hvals(kv_table_t *db, const char *key, char ***out, size_t *count) {
    if (!wr_obj(db, key, OBJ_HASH)) { *count=0; *out=NULL; return 0; }
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    ht_items((simple_ht_t*)o->ptr, out, count);
    return 0;
}

int kv_hgetall(kv_table_t *db, const char *key, char ***out, size_t *count) {
    if (!wr_obj(db, key, OBJ_HASH)) { *count=0; *out=NULL; return 0; }
    db_object_t *o = wr_obj(db, key, OBJ_HASH);
    size_t n = ((simple_ht_t*)o->ptr)->count;
    *out = malloc(n * 2 * sizeof(char*));
    if (!*out) return -1;
    size_t j = 0;
    char **ks = NULL; size_t kc = 0;
    ht_keys((simple_ht_t*)o->ptr, &ks, &kc);
    for (size_t i = 0; i < kc; ++i) {
        char *v = ht_get((simple_ht_t*)o->ptr, ks[i]);
        (*out)[j++] = ks[i];
        (*out)[j++] = v ? strdup(v) : strdup("");
    }
    free(ks);
    *count = j;
    return 0;
}

/* ================================================================== */
/*  Sorted Set commands                                                */
/* ================================================================== */
int kv_zadd(kv_table_t *db, const char *key, double score,
            const char *member, int *added) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) {
        o = obj_create_zset();
        if (kv_set_object(db, key, o, NULL) != 0) { obj_free(o); return -1; }
        o = kv_get_object(db, key);
    }
    zskiplist_t *z = (zskiplist_t*)o->ptr;
    zskiplist_node_t *n = zsl_insert(z, score, member);
    if (added) *added = n ? 1 : 0;
    return 0;
}

int kv_zrem(kv_table_t *db, const char *key, const char *member,
            int *removed) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) { if (removed) *removed = 0; return 0; }
    zskiplist_t *z = (zskiplist_t*)o->ptr;
    int r = zsl_delete(z, 0.0, member);
    if (removed) *removed = r;
    return 0;
}

double kv_zscore(kv_table_t *db, const char *key, const char *member,
                 int *found) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) { if (found) *found = 0; return 0.0; }
    zskiplist_t *z = (zskiplist_t*)o->ptr;
    zskiplist_node_t *n = z->header->forward[0];
    while (n) {
        if (strcmp(n->member, member) == 0) {
            if (found) *found = 1;
            return n->score;
        }
        n = n->forward[0];
    }
    if (found) *found = 0;
    return 0.0;
}

int kv_zcard(kv_table_t *db, const char *key, size_t *out) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    *out = o ? ((zskiplist_t*)o->ptr)->length : 0;
    return 0;
}

int kv_zrank(kv_table_t *db, const char *key, const char *member,
             int64_t *rank) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) { *rank = -1; return 0; }
    zskiplist_t *z = (zskiplist_t*)o->ptr;
    double sc = 0.0;
    int found = 0;
    zskiplist_node_t *n = z->header->forward[0];
    while (n) {
        if (strcmp(n->member, member) == 0) { sc = n->score; found = 1; break; }
        n = n->forward[0];
    }
    if (!found) { *rank = -1; return 0; }
    *rank = zsl_rank(z, sc, member);
    return 0;
}

int kv_zrevrank(kv_table_t *db, const char *key, const char *member,
                int64_t *rank) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) { *rank = -1; return 0; }
    double sc;
    int found;
    sc = kv_zscore(db, key, member, &found);
    if (!found) { *rank = -1; return 0; }
    *rank = zsl_revrank((zskiplist_t*)o->ptr, sc, member);
    return 0;
}

int kv_zrange(kv_table_t *db, const char *key, int64_t start,
              int64_t stop, char ***out, size_t *count) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) { *count = 0; *out = NULL; return 0; }
    zskiplist_t *z = (zskiplist_t*)o->ptr;
    int64_t len = (int64_t)z->length;
    if (start < 0) start += len;
    if (stop  < 0) stop  += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop) { *count=0; *out=NULL; return 0; }

    size_t n = (size_t)(stop - start + 1);
    *out = malloc(n * sizeof(char*));
    size_t j = 0;
    zskiplist_node_t *node = z->header->forward[0];
    for (int64_t i = 0; node && j < n; ++i) {
        if (i >= start) { (*out)[j++] = strdup(node->member); }
        node = node->forward[0];
    }
    *count = j;
    return 0;
}

int kv_zrevrange(kv_table_t *db, const char *key, int64_t start,
                 int64_t stop, char ***out, size_t *count) {
    db_object_t *o = wr_obj(db, key, OBJ_ZSET);
    if (!o) { *count = 0; *out = NULL; return 0; }
    zskiplist_t *z = (zskiplist_t*)o->ptr;
    int64_t len = (int64_t)z->length;
    if (start < 0) start += len;
    if (stop  < 0) stop  += len;
    if (start < 0) start = 0;
    if (stop >= len) stop = len - 1;
    if (start > stop) { *count=0; *out=NULL; return 0; }

    size_t n = (size_t)(stop - start + 1);
    *out = malloc(n * sizeof(char*));
    size_t j = 0;
    zskiplist_node_t *node = z->tail;
    for (int64_t i = 0; node && j < n; ++i) {
        if (i >= start) { (*out)[j++] = strdup(node->member); }
        node = (i < len - 1) ? node->backward : NULL;
    }
    *count = j;
    return 0;
}

/* ================================================================== */
/*  Pub / Sub                                                          */
/* ================================================================== */

pubsub_mgr_t *pubsub_create(void) {
    pubsub_mgr_t *m = calloc(1, sizeof(*m));
    if (m) pthread_rwlock_init(&m->lock, NULL);
    return m;
}

void pubsub_destroy(pubsub_mgr_t *m) {
    if (!m) return;
    pthread_rwlock_wrlock(&m->lock);
    channel_entry_t *ch = m->channels;
    while (ch) {
        channel_entry_t *next = ch->next;
        client_sub_t *cs = ch->subs;
        while (cs) {
            client_sub_t *ns = cs->next;
            free(cs);
            cs = ns;
        }
        free(ch);
        ch = next;
    }
    pthread_rwlock_unlock(&m->lock);
    pthread_rwlock_destroy(&m->lock);
    free(m);
}

static channel_entry_t *ps_get_or_create(pubsub_mgr_t *m, const char *ch) {
    channel_entry_t *cur = m->channels;
    while (cur) {
        if (strcmp(cur->name, ch) == 0) return cur;
        cur = cur->next;
    }
    channel_entry_t *n = calloc(1, sizeof(*n));
    if (!n) return NULL;
    strncpy(n->name, ch, CHANNEL_NAME_MAX - 1);
    n->name[CHANNEL_NAME_MAX - 1] = '\0';
    n->subs = NULL;
    n->next = m->channels;
    m->channels = n;
    return n;
}

int pubsub_subscribe(pubsub_mgr_t *m, int fd, pthread_mutex_t *wlock,
                     const char *channel) {
    pthread_rwlock_wrlock(&m->lock);
    channel_entry_t *ch = ps_get_or_create(m, channel);
    if (!ch) { pthread_rwlock_unlock(&m->lock); return -1; }
    client_sub_t *cs = calloc(1, sizeof(*cs));
    if (!cs) { pthread_rwlock_unlock(&m->lock); return -1; }
    cs->fd    = fd;
    cs->wlock = wlock;
    cs->next  = ch->subs;
    ch->subs  = cs;
    pthread_rwlock_unlock(&m->lock);
    return 0;
}

int pubsub_unsubscribe(pubsub_mgr_t *m, int fd, const char *channel) {
    pthread_rwlock_wrlock(&m->lock);
    channel_entry_t *ch = m->channels;
    client_sub_t **prev = NULL;
    while (ch) {
        if (strcmp(ch->name, channel) == 0) {
            prev = &ch->subs;
            client_sub_t *cs = ch->subs;
            while (cs) {
                if (cs->fd == fd) {
                    *prev = cs->next;
                    free(cs);
                    break;
                }
                prev = &cs->next;
                cs = cs->next;
            }
            break;
        }
        ch = ch->next;
    }
    pthread_rwlock_unlock(&m->lock);
    return 0;
}

void pubsub_unsubscribe_all(pubsub_mgr_t *m, int fd) {
    pthread_rwlock_wrlock(&m->lock);
    channel_entry_t *ch = m->channels;
    while (ch) {
        client_sub_t **prev = &ch->subs;
        client_sub_t *cs = ch->subs;
        while (cs) {
            if (cs->fd == fd) {
                *prev = cs->next;
                free(cs);
                cs = *prev;
                continue;
            }
            prev = &cs->next;
            cs = cs->next;
        }
        ch = ch->next;
    }
    pthread_rwlock_unlock(&m->lock);
}

int pubsub_publish(pubsub_mgr_t *m, const char *channel, const char *msg) {
    int count = 0;
    pthread_rwlock_rdlock(&m->lock);
    channel_entry_t *ch = m->channels;
    while (ch) {
        if (strcmp(ch->name, channel) == 0) {
            client_sub_t *cs = ch->subs;
            while (cs) {
                if (cs->wlock) pthread_mutex_lock(cs->wlock);
                char buf[BULK_MSG_MAX];
                int len = snprintf(buf, sizeof(buf),
                                   "*3\r\n$7\r\nmessage\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                                   strlen(channel), channel,
                                   strlen(msg), msg);
                if (cs->fd >= 0) {
                    ssize_t sent = send(cs->fd, buf, len,
                                        MSG_NOSIGNAL);
                    if (sent >= 0) ++count;
                }
                if (cs->wlock) pthread_mutex_unlock(cs->wlock);
                cs = cs->next;
            }
            break;
        }
        ch = ch->next;
    }
    pthread_rwlock_unlock(&m->lock);
    return count;
}

int pubsub_numsub(pubsub_mgr_t *m, const char *channel) {
    int c = 0;
    pthread_rwlock_rdlock(&m->lock);
    channel_entry_t *ch = m->channels;
    while (ch) {
        if (strcmp(ch->name, channel) == 0) {
            client_sub_t *cs = ch->subs;
            while (cs) { ++c; cs = cs->next; }
            break;
        }
        ch = ch->next;
    }
    pthread_rwlock_unlock(&m->lock);
    return c;
}

int pubsub_numpat(pubsub_mgr_t *m) {
    (void)m; return 0;
}

/* ================================================================== */
/*  DATA STRUCTURES IMPLEMENTATION                                    */
/* ================================================================== */
/* --------------------------- dlist ------------------------------ */
static void dlist_push_left(dlist_t *lst, const char *val) {
    list_node_t *n = calloc(1, sizeof(*n));
    n->value = strdup(val);
    if (!lst->head) { lst->head = lst->tail = n; }
    else {
        n->next = lst->head;
        lst->head->prev = n;
        lst->head = n;
    }
    ++lst->len;
}

static void dlist_push_right(dlist_t *lst, const char *val) {
    list_node_t *n = calloc(1, sizeof(*n));
    n->value = strdup(val);
    if (!lst->tail) { lst->head = lst->tail = n; }
    else {
        n->prev = lst->tail;
        lst->tail->next = n;
        lst->tail = n;
    }
    ++lst->len;
}

static char *dlist_pop_left(dlist_t *lst) {
    if (!lst->head) return NULL;
    list_node_t *n = lst->head;
    char *v = n->value;
    lst->head = n->next;
    if (lst->head) lst->head->prev = NULL;
    else lst->tail = NULL;
    free(n);
    --lst->len;
    return v;
}

static char *dlist_pop_right(dlist_t *lst) {
    if (!lst->tail) return NULL;
    list_node_t *n = lst->tail;
    char *v = n->value;
    lst->tail = n->prev;
    if (lst->tail) lst->tail->next = NULL;
    else lst->head = NULL;
    free(n);
    --lst->len;
    return v;
}

static char *dlist_get(dlist_t *lst, int64_t idx) {
    int64_t len = (int64_t)lst->len;
    if (idx < 0) idx += len;
    if (idx < 0 || idx >= len) return NULL;
    list_node_t *n = lst->head;
    for (int64_t i = 0; n && i < idx; ++i) n = n->next;
    return n ? strdup(n->value) : NULL;
}

static int dlist_set(dlist_t *lst, int64_t idx, const char *val) {
    int64_t len = (int64_t)lst->len;
    if (idx < 0) idx += len;
    if (idx < 0 || idx >= len) return -1;
    list_node_t *n = lst->head;
    for (int64_t i = 0; n && i < idx; ++i) n = n->next;
    if (!n) return -1;
    free(n->value);
    n->value = strdup(val);
    return 0;
}

static int dlist_rem(dlist_t *lst, int64_t count, const char *val) {
    int removed = 0;
    list_node_t *n = lst->head;
    while (n) {
        list_node_t *next = n->next;
        if (strcmp(n->value, val) == 0) {
            if (n->prev) n->prev->next = n->next;
            else lst->head = n->next;
            if (n->next) n->next->prev = n->prev;
            else lst->tail = n->prev;
            free(n->value); free(n);
            ++removed; --lst->len;
            if (count > 0 && removed >= count) break;
        }
        n = next;
    }
    return removed;
}

static void dlist_free(dlist_t *lst) {
    list_node_t *n = lst->head;
    while (n) {
        list_node_t *nn = n->next;
        free(n->value); free(n);
        n = nn;
    }
    free(lst);
}

/* --------------------------- simple hash table ------------------ */
#define HT_SMALL_CAP 16

static simple_ht_t *ht_create(void) {
    simple_ht_t *ht = calloc(1, sizeof(*ht));
    if (!ht) return NULL;
    ht->cap = HT_SMALL_CAP;
    ht->buckets = calloc(ht->cap, sizeof(sht_node_t*));
    if (!ht->buckets) { free(ht); return NULL; }
    return ht;
}

static uint64_t ht_fn(const char *s) {
    return fnv1a(s);
}

static sht_node_t **ht_find_prev(simple_ht_t *ht, const char *key) {
    size_t idx = ht_fn(key) % ht->cap;
    sht_node_t **pp = &ht->buckets[idx];
    while (*pp) {
        if (strcmp((*pp)->key, key) == 0) return pp;
        pp = &(*pp)->next;
    }
    return pp;  /* points to the location where new node would go */
}

static void ht_resize(simple_ht_t *ht) {
    if ((double)ht->count / ht->cap < 0.75) return;
    size_t old_cap = ht->cap;
    sht_node_t **old_buckets = ht->buckets;
    ht->cap *= 2;
    ht->count = 0;
    ht->buckets = calloc(ht->cap, sizeof(sht_node_t*));
    for (size_t i = 0; i < old_cap; ++i) {
        sht_node_t *n = old_buckets[i];
        while (n) {
            sht_node_t *nn = n->next;
            ht_add(ht, n->key, n->value, 0);
            free(n->key);
            free(n->value);
            free(n);
            n = nn;
        }
    }
    free(old_buckets);
}

static int ht_add(simple_ht_t *ht, const char *key, const char *val,
                  int replace) {
    ht_resize(ht);
    sht_node_t **pp = ht_find_prev(ht, key);
    if (*pp) {
        if (!replace) return 0;   /* already member, not an add */
        free((*pp)->value);
        (*pp)->value = val ? strdup(val) : NULL;
        return 0;
    }
    sht_node_t *n = calloc(1, sizeof(*n));
    n->key  = strdup(key);
    n->value = val ? strdup(val) : NULL;
    n->next = NULL;
    *pp = n;
    ++ht->count;
    return 1;
}

static int ht_remove(simple_ht_t *ht, const char *key) {
    sht_node_t **pp = ht_find_prev(ht, key);
    if (!*pp) return 0;
    sht_node_t *n = *pp;
    *pp = n->next;
    free(n->key); free(n->value); free(n);
    --ht->count;
    return 1;
}

static int ht_exists(simple_ht_t *ht, const char *key) {
    sht_node_t **pp = ht_find_prev(ht, key);
    return (*pp) ? 1 : 0;
}

static char *ht_get(simple_ht_t *ht, const char *key) {
    sht_node_t **pp = ht_find_prev(ht, key);
    return (*pp) ? (*pp)->value : NULL;
}

static void ht_keys(simple_ht_t *ht, char ***out, size_t *count) {
    *out    = malloc(ht->count * sizeof(char*));
    size_t j = 0;
    for (size_t i = 0; i < ht->cap; ++i) {
        sht_node_t *n = ht->buckets[i];
        while (n) {
            if (j < ht->count) (*out)[j++] = strdup(n->key);
            n = n->next;
        }
    }
    *count = j;
}

static void ht_items(simple_ht_t *ht, char ***out, size_t *count) {
    *out = malloc(ht->count * sizeof(char*));
    size_t j = 0;
    for (size_t i = 0; i < ht->cap; ++i) {
        sht_node_t *n = ht->buckets[i];
        while (n) {
            if (j < ht->count)
                (*out)[j++] = n->value ? strdup(n->value) : strdup("");
            n = n->next;
        }
    }
    *count = j;
}

static void ht_free(simple_ht_t *ht) {
    for (size_t i = 0; i < ht->cap; ++i) {
        sht_node_t *n = ht->buckets[i];
        while (n) {
            sht_node_t *nn = n->next;
            free(n->key); free(n->value); free(n);
            n = nn;
        }
    }
    free(ht->buckets);
    free(ht);
}

/* --------------------------- skip list -------------------------- */
/* Level random (power-law, p=1/4) */
static unsigned zsl_random_level(void) {
    unsigned level = 1;
    while ((rand() & 3) == 0 && level < ZSKIPLIST_MAXLEVEL) ++level;
    return level;
}

static zskiplist_t *zsl_create(void) {
    zskiplist_t *z = calloc(1, sizeof(*z));
    if (!z) return NULL;
    z->level = 1;
    z->header = calloc(1, sizeof(zskiplist_node_t));
    z->header->forward = calloc(ZSKIPLIST_MAXLEVEL,
                                 sizeof(zskiplist_node_t*));
    z->update_buf = malloc(ZSKIPLIST_MAXLEVEL * sizeof(zskiplist_node_t*));
    z->rank_buf   = malloc(ZSKIPLIST_MAXLEVEL * sizeof(unsigned));
    memset(z->header->forward, 0,
           ZSKIPLIST_MAXLEVEL * sizeof(zskiplist_node_t*));
    return z;
}

static zskiplist_node_t *zsl_insert(zskiplist_t *z, double score,
                                    const char *member) {
    zskiplist_node_t **update = z->update_buf;
    unsigned *rank             = z->rank_buf;
    zskiplist_node_t *x = z->header;
    for (int lvl = (int)z->level - 1; lvl >= 0; --lvl) {
        rank[lvl] = (unsigned)((lvl == (int)z->level - 1) ? 0 : rank[lvl + 1]);
        while (x->forward[lvl] &&
               (x->forward[lvl]->score < score ||
                (x->forward[lvl]->score == score &&
                 strcmp(x->forward[lvl]->member, member) < 0))) {
            rank[lvl] += (unsigned)(1);        /* simplistic */
            x = x->forward[lvl];
        }
        update[lvl] = x;
    }

    unsigned lvl = zsl_random_level();
    if (lvl > z->level) {
        for (unsigned i = z->level; i < lvl; ++i) {
            rank[i] = 0;
            update[i] = z->header;
        }
        z->level = lvl;
    }

    zskiplist_node_t *n = calloc(1, sizeof(*n));
    n->member   = strdup(member);
    n->score    = score;
    n->level    = lvl;
    n->forward  = calloc(lvl, sizeof(zskiplist_node_t*));

    for (unsigned i = 0; i < lvl; ++i) {
        n->forward[i]    = update[i]->forward[i];
        update[i]->forward[i] = n;
    }
    n->backward = (update[0] == z->header) ? NULL : update[0];
    if (n->forward[0])
        n->forward[0]->backward = n;
    else
        z->tail = n;
    ++z->length;
    return n;
}

static int zsl_delete(zskiplist_t *z, double score, const char *member) {
    (void)score;
    zskiplist_node_t **update = z->update_buf;
    zskiplist_node_t *x = z->header;
    for (int lvl = (int)z->level - 1; lvl >= 0; --lvl) {
        while (x->forward[lvl] &&
               strcmp(x->forward[lvl]->member, member) != 0) {
            x = x->forward[lvl];
        }
        update[lvl] = x;
    }
    x = x->forward[0];
    if (!x || strcmp(x->member, member) != 0) return 0;

    for (unsigned i = 0; i < z->level; ++i) {
        if (update[i]->forward[i] == x)
            update[i]->forward[i] = x->forward[i];
    }
    if (x->forward[0]) x->forward[0]->backward = x->backward;
    else z->tail = x->backward;
    while (z->level > 1 && z->header->forward[z->level - 1] == NULL)
        --z->level;
    free(x->member); free(x->forward); free(x);
    --z->length;
    return 1;
}

/* rank = # elements with lower score, or lower score + lexicographic */
static int zsl_rank(const zskiplist_t *z, double score,
                    const char *member) {
    int rank = 0;
    zskiplist_node_t *n = z->header->forward[0];
    while (n) {
        if (n->score < score ||
            (n->score == score && strcmp(n->member, member) < 0)) {
            ++rank;
        } else break;
        n = n->forward[0];
    }
    return rank;
}

static int zsl_revrank(const zskiplist_t *z, double score,
                       const char *member) {
    int rank = 0;
    zskiplist_node_t *n = z->tail;
    while (n) {
        if (n->score > score ||
            (n->score == score && strcmp(n->member, member) > 0)) {
            ++rank;
        } else break;
        n = n->backward;
    }
    return rank;
}

static void zsl_free(zskiplist_t *z) {
    zskiplist_node_t *n = z->header->forward[0], *nn;
    while (n) {
        nn = n->forward[0];
        free(n->member); free(n->forward); free(n);
        n = nn;
    }
    free(z->header->forward); free(z->header);
    free(z->update_buf); free(z->rank_buf);
    free(z);
}
