#ifndef KVDB_H
#define KVDB_H

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/socket.h>

#define OBJ_STRING  0
#define OBJ_LIST    1
#define OBJ_SET     2
#define OBJ_HASH    3
#define OBJ_ZSET    4

#define NODE_ORIGIN_LEN  16
#define CHANNEL_NAME_MAX 256
#define BULK_MSG_MAX     8192

/* ------------------------------------------------------------------ */
/*  Metadata & Object model                                            */
/* ------------------------------------------------------------------ */
typedef struct {
    uint64_t version;
    uint64_t wallclock;
    char     origin[NODE_ORIGIN_LEN];
} kv_meta_t;

/* Forward declarations for the polymorphic object */
struct kv_table;
struct dlist;
struct dset;
struct dhash;
struct zskiplist;

typedef struct db_object {
    int   type;               /* OBJ_STRING / LIST / SET / HASH / ZSET */
    void *ptr;                /* concrete data-structure pointer        */
    int64_t   ttl;            /* -1 = no expiry, >=0 = unix-msec expiry */
    uint64_t  version;
    uint64_t  wallclock;
    char      origin[NODE_ORIGIN_LEN];
} db_object_t;

/* Opaque database handle */
typedef struct kv_table kv_table_t;

/* ------------------------------------------------------------------ */
/*  Object lifecycle                                                   */
/* ------------------------------------------------------------------ */
db_object_t *obj_create_string(const char *s);
db_object_t *obj_create_list(void);
db_object_t *obj_create_set(void);
db_object_t *obj_create_hash(void);
db_object_t *obj_create_zset(void);
void         obj_free(db_object_t *o);

/* Return value as a malloc'd string for RESP / text protocol */
char *obj_to_string(const db_object_t *o);

/* ------------------------------------------------------------------ */
/*  Database open / close / save                                       */
/* ------------------------------------------------------------------ */
kv_table_t *kv_open(const char *path);
void        kv_close(kv_table_t *db);
int         kv_save(kv_table_t *db);
uint64_t    kv_next_version(kv_table_t *db);
uint64_t    kv_db_version(kv_table_t *db);

/* ------------------------------------------------------------------ */
/*  Generic key-space CRUD (object-level)                              */
/* ------------------------------------------------------------------ */
int  kv_set_object(kv_table_t *db, const char *key, db_object_t *obj,
                   const kv_meta_t *meta);
db_object_t *kv_get_object(kv_table_t *db, const char *key);
int  kv_del(kv_table_t *db, const char *key);

/* High-level typed helpers ------------------------------------------- */
/* They return 0 on success, -1 on error. String helpers return
   allocated char* or NULL. */

/* String */
int  kv_set(kv_table_t *db, const char *key, const char *value);
char *kv_get(kv_table_t *db, const char *key);
int  kv_set_meta(kv_table_t *db, const char *key, const char *value,
                 const kv_meta_t *meta);
int  kv_del_meta(kv_table_t *db, const char *key, const kv_meta_t *meta);
char *kv_get_meta(kv_table_t *db, const char *key, kv_meta_t *out_meta);

int  kv_exists(kv_table_t *db, const char *key);
int  kv_keys(kv_table_t *db, char ***out_keys, size_t *out_count);
void kv_keys_free(char **keys, size_t count);

/* TTL */
int64_t    kv_ttl_ms(kv_table_t *db, const char *key);
int        kv_set_ttl(kv_table_t *db, const char *key, int64_t ttl);
int        kv_expire(kv_table_t *db, const char *key, int64_t msec);
int        kv_persist(kv_table_t *db, const char *key);
size_t     kv_active_expire(kv_table_t *db, size_t max_samples,
                             int64_t now_msec);

/* String-ops (require existing OBJ_STRING, return -1 on type mismatch) */
int   kv_incr(kv_table_t *db, const char *key, int64_t *out);
int   kv_incrby(kv_table_t *db, const char *key, int64_t delta,
                int64_t *out);
int   kv_decr(kv_table_t *db, const char *key, int64_t *out);
int   kv_decrby(kv_table_t *db, const char *key, int64_t delta,
                int64_t *out);
int   kv_append(kv_table_t *db, const char *key, const char *suffix,
                size_t *out_len);
char *kv_getrange(kv_table_t *db, const char *key, int64_t start,
                  int64_t end);
int   kv_strlen(kv_table_t *db, const char *key, size_t *out);
int   kv_setnx(kv_table_t *db, const char *key, const char *value);
int   kv_setex(kv_table_t *db, const char *key, const char *value,
               int64_t msec);

/* ------------------------------------------------------------------ */
/*  List commands  (key → OBJ_LIST)                                    */
/* ------------------------------------------------------------------ */
int   kv_lpush(kv_table_t *db, const char *key, const char *value,
               size_t *out_len);
int   kv_rpush(kv_table_t *db, const char *key, const char *value,
               size_t *out_len);
char *kv_lpop(kv_table_t *db, const char *key);
char *kv_rpop(kv_table_t *db, const char *key);
int   kv_llen(kv_table_t *db, const char *key, size_t *out);
char *kv_lindex(kv_table_t *db, const char *key, int64_t idx);
int   kv_lset(kv_table_t *db, const char *key, int64_t idx,
              const char *value);
int   kv_lrem(kv_table_t *db, const char *key, int64_t count,
              const char *value, size_t *removed);
char *kv_lpoprpush(kv_table_t *db, const char *src, const char *dst);

/* ------------------------------------------------------------------ */
/*  Set commands   (key → OBJ_SET)                                     */
/* ------------------------------------------------------------------ */
int   kv_sadd(kv_table_t *db, const char *key,
             const char **members, int member_count, int *added);
int   kv_srem(kv_table_t *db, const char *key,
             const char **members, int member_count, int *removed);
int   kv_sismember(kv_table_t *db, const char *key, const char *member);
int   kv_scard(kv_table_t *db, const char *key, size_t *out);

/* Returns malloc'd array of strings, *count elements. Caller frees
   each string + the array. */
int   kv_smembers(kv_table_t *db, const char *key,
                  char ***out, size_t *count);
void  kv_free_array(char **arr, size_t count);

/* ------------------------------------------------------------------ */
/*  Hash commands  (key → OBJ_HASH)                                    */
/* ------------------------------------------------------------------ */
int   kv_hset(kv_table_t *db, const char *key, const char *field,
              const char *value, int *created);
char *kv_hget(kv_table_t *db, const char *key, const char *field);
int   kv_hdel(kv_table_t *db, const char *key,
             const char **fields, int field_count, int *removed);
int   kv_hexists(kv_table_t *db, const char *key, const char *field);
int   kv_hlen(kv_table_t *db, const char *key, size_t *out);
int   kv_hkeys(kv_table_t *db, const char *key,
               char ***out, size_t *count);
int   kv_hvals(kv_table_t *db, const char *key,
               char ***out, size_t *count);
int   kv_hgetall(kv_table_t *db, const char *key,
                 char ***out, size_t *count);   /* field,val,field,val... */

/* ------------------------------------------------------------------ */
/*  Sorted-Set commands  (key → OBJ_ZSET, skip-list)                   */
/* ------------------------------------------------------------------ */
int   kv_zadd(kv_table_t *db, const char *key, double score,
              const char *member, int *added);
int   kv_zrem(kv_table_t *db, const char *key, const char *member,
              int *removed);
double kv_zscore(kv_table_t *db, const char *key, const char *member,
                 int *found);
int   kv_zcard(kv_table_t *db, const char *key, size_t *out);
int   kv_zrank(kv_table_t *db, const char *key, const char *member,
               int64_t *rank);         /* 0-based asc, -1 if missing */
int   kv_zrevrank(kv_table_t *db, const char *key, const char *member,
                  int64_t *rank);

/* Both return malloc'd arrays in *out, with *count entries */
int   kv_zrange(kv_table_t *db, const char *key, int64_t start,
                int64_t stop, char ***out, size_t *count);
int   kv_zrevrange(kv_table_t *db, const char *key, int64_t start,
                   int64_t stop, char ***out, size_t *count);

/* ------------------------------------------------------------------ */
/*  Pub / Sub                                                          */
/* ------------------------------------------------------------------ */

typedef struct pubsub_mgr pubsub_mgr_t;

pubsub_mgr_t *pubsub_create(void);
void          pubsub_destroy(pubsub_mgr_t *mgr);

/* Channel subscriptions. `fd` is the client socket. `wlock` is the
   per-client write mutex (owned by caller) used to serialise writes
   from the publisher side. */
int  pubsub_subscribe(pubsub_mgr_t *mgr, int fd, pthread_mutex_t *wlock,
                      const char *channel);
int  pubsub_unsubscribe(pubsub_mgr_t *mgr, int fd, const char *channel);
void pubsub_unsubscribe_all(pubsub_mgr_t *mgr, int fd);

/* Publish returns the number of clients that received the message. */
int  pubsub_publish(pubsub_mgr_t *mgr, const char *channel,
                    const char *message);

/* Return the number of subscribers for a channel, or total channels */
int  pubsub_numsub(pubsub_mgr_t *mgr, const char *channel);
int  pubsub_numpat(pubsub_mgr_t *mgr);   /* patterns (stub) */

#endif
