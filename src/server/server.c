#include "kvdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/time.h>
#include <ctype.h>

#ifdef CLUSTER_ENABLED
#include "cluster/conf.h"
#include "cluster/cluster.h"
#endif

#define DEFAULT_PORT     6379
#define BUFFER_SIZE      4096
#define MAX_ARGS         64
#define THREAD_POOL_SIZE 8
#define EXPIRE_INTERVAL_MS 100

/* ------------------------------------------------------------------ */
/*  Thread Pool                                                        */
/* ------------------------------------------------------------------ */
typedef struct tp_job {
    int   fd;
    struct tp_job *next;
} tp_job_t;

typedef struct {
    tp_job_t        *head, *tail;
    pthread_mutex_t  lock;
    pthread_cond_t   cond;
    int              stop;
    pthread_t        workers[THREAD_POOL_SIZE];
} thread_pool_t;

static thread_pool_t g_tp;

#ifdef CLUSTER_ENABLED
static cluster_state_t *g_cs = NULL;
static char g_my_id[16] = {0};
static void load_or_create_nodeid_from_config(const char *base, int port,
                                              const char *id_from_config) {
    char path[256];
    snprintf(path, sizeof(path), "%s.%d", base, port);
    FILE *fp = fopen(path, "r");
    if (fp) {
        if (fgets(g_my_id, sizeof(g_my_id), fp))
            g_my_id[strcspn(g_my_id, "\r\n")] = '\0';
        fclose(fp);
        if (id_from_config && strcmp(g_my_id, id_from_config) != 0) {
            strncpy(g_my_id, id_from_config, 15);
            g_my_id[15] = '\0';
            fp = fopen(path, "w");
            if (fp) { fprintf(fp, "%s\n", g_my_id); fclose(fp); }
        }
        return;
    }
    if (id_from_config && id_from_config[0]) {
        strncpy(g_my_id, id_from_config, 15);
        g_my_id[15] = '\0';
    } else {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        snprintf(g_my_id, sizeof(g_my_id), "n%05x",
                 (int)(tv.tv_usec % 0x100000));
    }
    fp = fopen(path, "w");
    if (fp) { fprintf(fp, "%s\n", g_my_id); fclose(fp); }
}
#endif

static kv_table_t    *g_db = NULL;
static pubsub_mgr_t  *g_ps = NULL;
static int            g_running = 1;

static void handle_sigint(int sig) {
    (void)sig;
    g_running = 0;
}

/* ---------- RESP / text protocol ---------- */
static void send_raw(int fd, const char *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) break;
        sent += (size_t)n;
    }
}

static void send_reply(int fd, const char *msg) {
    size_t len = strlen(msg);
    size_t buflen = len + 3;
    char *buf = malloc(buflen);
    memcpy(buf, msg, len);
    buf[len] = '\r'; buf[len + 1] = '\n'; buf[len + 2] = '\0';
    send_raw(fd, buf, len + 2);
    free(buf);
}

static void send_int(int fd, int64_t n) {
    char buf[64];
    snprintf(buf, sizeof(buf), ":%lld", (long long)n);
    send_reply(fd, buf);
}

static void send_bulk(int fd, const char *s) {
    if (!s) { send_reply(fd, "$-1"); return; }
    char buf[BULK_MSG_MAX];
    int len = snprintf(buf, sizeof(buf), "$%zu\r\n%s", strlen(s), s);
    send_raw(fd, buf, (size_t)len);
    send_raw(fd, "\r\n", 2);
}

static void send_array_len(int fd, size_t n) {
    char buf[32];
    snprintf(buf, sizeof(buf), "*%zu", n);
    send_reply(fd, buf);
}

static void send_err(int fd, const char *msg) {
    if (!msg) msg = "ERR";
    char buf[512];
    snprintf(buf, sizeof(buf), "-%s", msg);
    send_reply(fd, buf);
}

static void send_ok(int fd) {
    send_reply(fd, "+OK");
}

static void send_pong(int fd) {
    send_reply(fd, "+PONG");
}

static void send_nil(int fd) {
    send_reply(fd, "$-1");
}

/* ---------- argument parsing ---------- */
static int parse_line(char *buf, char **args, int max_args) {
    int argc = 0;
    char *saveptr = NULL;
    char *token = strtok_r(buf, " \t\r", &saveptr);
    while (token && argc < max_args) {
        args[argc++] = token;
        token = strtok_r(NULL, " \t\r", &saveptr);
    }
    return argc;
}

static long long arg_i(char **args, int idx) {
    if (idx < 0) return 0;
    return strtoll(args[idx], NULL, 10);
}

/* ---------- command routing ---------- */
static inline void reply_int(int fd, int64_t n) { send_int(fd, n); }
static inline void reply_bulk(int fd, const char *s) { send_bulk(fd, s); }
static inline void reply_ok(int fd) { send_ok(fd); }
static inline void reply_nil(int fd) { send_nil(fd); }
static inline void reply_pong(int fd) { send_pong(fd); }
static inline void reply_err(int fd, const char *s) { send_err(fd, s); }

static void process_command(int fd, char *line) {
    char *args[MAX_ARGS];
    int argc = parse_line(line, args, MAX_ARGS);
    if (argc == 0) return;

    const char *cmd = args[0];

    /* ---- connection ---- */
    if (strcasecmp(cmd, "PING") == 0) {
        if (argc >= 2) send_bulk(fd, args[1]);
        else reply_pong(fd);
        return;
    }
    if (strcasecmp(cmd, "QUIT") == 0) {
        reply_ok(fd); return;
    }

    /* ---- generic key-space ---- */
    if (strcasecmp(cmd, "SET") == 0 && argc >= 3) {
        const char *k = args[1], *v = args[2];
        if (kv_set(g_db, k, v) == 0) {
            /* TTL parsing: SET k v EX|PX seconds|milliseconds */
            int i = 3;
            while (i+1 < argc) {
                if (strcasecmp(args[i], "EX") == 0) {
                    struct timeval tv;
                    gettimeofday(&tv, NULL);
                    int64_t now = (int64_t)tv.tv_sec * 1000LL +
                                  (int64_t)tv.tv_usec / 1000LL;
                    kv_set_ttl(g_db, k, now + arg_i(args, i+1) * 1000);
                    i += 2;
                } else if (strcasecmp(args[i], "PX") == 0) {
                    int64_t now;
                    { struct timeval tv; gettimeofday(&tv,NULL);
                      now = tv.tv_sec*1000LL+tv.tv_usec/1000LL; }
                    kv_set_ttl(g_db, k, now + arg_i(args,i+1));
                    i += 2;
                } else { break; }
            }
#ifdef CLUSTER_ENABLED
            if (g_cs) {
                kv_meta_t m; char *ck = kv_get_meta(g_db,k,&m);
                if (ck) { memcpy(m.origin,g_my_id,15);m.origin[15]='\0';
                          cluster_replicate_set(g_cs,k,v,&m); free(ck); }
            }
#endif
            reply_ok(fd);
        } else reply_err(fd, "ERR");
        return;
    }
    if (strcasecmp(cmd, "SETNX") == 0 && argc >= 3) {
        int r = kv_setnx(g_db, args[1], args[2]);
        reply_int(fd, r == 0 ? 1 : 0); return;
    }
    if (strcasecmp(cmd, "SETEX") == 0 && argc >= 4) {
        long long t = arg_i(args, 2);
        int r = kv_setex(g_db, args[1], args[3], t * 1000);
        reply_ok(fd); (void)r; return;
    }
    if (strcasecmp(cmd, "GET") == 0 && argc >= 2) {
        char *v = kv_get(g_db, args[1]);
        reply_bulk(fd, v); free(v); return;
    }
    if (strcasecmp(cmd, "GETRANGE") == 0 && argc >= 4) {
        long long s = arg_i(args, 2), e = arg_i(args, 3);
        char *v = kv_getrange(g_db, args[1], s, e);
        reply_bulk(fd, v); free(v); return;
    }
    if (strcasecmp(cmd, "APPEND") == 0 && argc >= 3) {
        size_t len;
        if (kv_append(g_db, args[1], args[2], &len) == 0)
            reply_int(fd, (int64_t)len);
        else reply_err(fd, "ERR");
        return;
    }
    if (strcasecmp(cmd, "STRLEN") == 0 && argc >= 2) {
        size_t l;
        if (kv_strlen(g_db, args[1], &l) == 0) reply_int(fd,(int64_t)l);
        else reply_nil(fd);
        return;
    }
    if (strcasecmp(cmd, "INCR") == 0 && argc >= 2) {
        int64_t out;
        if (kv_incr(g_db, args[1], &out) == 0) reply_int(fd, out);
        else reply_err(fd,"ERR value is not an integer");
        return;
    }
    if (strcasecmp(cmd, "DECR") == 0 && argc >= 2) {
        int64_t out;
        if (kv_decr(g_db, args[1], &out) == 0) reply_int(fd, out);
        else reply_err(fd,"ERR value is not an integer");
        return;
    }
    if (strcasecmp(cmd, "INCRBY") == 0 && argc >= 3) {
        int64_t out;
        if (kv_incrby(g_db, args[1], arg_i(args,2), &out) == 0)
            reply_int(fd, out);
        else reply_err(fd,"ERR");
        return;
    }
    if (strcasecmp(cmd, "DECRBY") == 0 && argc >= 3) {
        int64_t out;
        if (kv_decrby(g_db, args[1], arg_i(args,2), &out) == 0)
            reply_int(fd, out);
        else reply_err(fd,"ERR");
        return;
    }
    if (strcasecmp(cmd, "DEL") == 0 && argc >= 2) {
        int ok = 0;
        for (int i=1; i<argc; ++i)
            if (kv_del(g_db, args[i]) == 0) ++ok;
        reply_int(fd, ok); return;
    }
    if (strcasecmp(cmd, "EXISTS") == 0 && argc >= 2) {
        int n = 0;
        for (int i=1; i<argc; ++i)
            if (kv_exists(g_db, args[i])) ++n;
        reply_int(fd, n); return;
    }
    if (strcasecmp(cmd, "TYPE") == 0 && argc >= 2) {
        db_object_t *obj = kv_get_object(g_db, args[1]);
        if (!obj) send_reply(fd, "+none"); else {
            switch (obj->type) {
            case OBJ_STRING: send_reply(fd, "+string"); break;
            case OBJ_LIST:   send_reply(fd, "+list");   break;
            case OBJ_SET:    send_reply(fd, "+set");    break;
            case OBJ_HASH:   send_reply(fd, "+hash");   break;
            case OBJ_ZSET:   send_reply(fd, "+zset");   break;
            }
        }
        return;
    }
    if (strcasecmp(cmd, "KEYS") == 0) {
        char **k = NULL; size_t kc = 0;
        if (kv_keys(g_db, &k, &kc) == 0) {
            send_array_len(fd, kc);
            for (size_t i=0; i<kc; ++i) { reply_bulk(fd,k[i]); }
            kv_keys_free(k, kc);
        } else reply_err(fd, "ERR");
        return;
    }
    if (strcasecmp(cmd, "SAVE") == 0) {
        if (kv_save(g_db) == 0) reply_ok(fd);
        else reply_err(fd, "ERR");
        return;
    }

    /* ---- TTL ---- */
    if (strcasecmp(cmd, "TTL") == 0 && argc >= 2) {
        int64_t t = kv_ttl_ms(g_db, args[1]);
        if (t == -2) reply_int(fd, -2);
        else if (t == -1) reply_int(fd, -1);
        else {
            struct timeval tv; gettimeofday(&tv,NULL);
            int64_t now = tv.tv_sec*1000LL+tv.tv_usec/1000LL;
            reply_int(fd, (t - now) / 1000);
        }
        return;
    }
    if (strcasecmp(cmd, "PTTL") == 0 && argc >= 2) {
        int64_t t = kv_ttl_ms(g_db, args[1]);
        if (t == -2) reply_int(fd, -2);
        else if (t == -1) reply_int(fd, -1);
        else {
            struct timeval tv; gettimeofday(&tv,NULL);
            int64_t now = tv.tv_sec*1000LL+tv.tv_usec/1000LL;
            reply_int(fd, t - now);
        }
        return;
    }
    if (strcasecmp(cmd, "EXPIRE") == 0 && argc >= 3) {
        long long ms = arg_i(args, 2) * 1000LL;
        if (kv_expire(g_db, args[1], ms) == 0) reply_int(fd,1);
        else reply_int(fd,0);
        return;
    }
    if (strcasecmp(cmd, "PERSIST") == 0 && argc >= 2) {
        reply_int(fd, kv_persist(g_db, args[1]) == 0 ? 1 : 0);
        return;
    }

    /* ---- LIST ---- */
    if (strcasecmp(cmd, "LPUSH") == 0 && argc >= 3) {
        size_t len;
        for (int i=2; i<argc; ++i)
            kv_lpush(g_db, args[1], args[i], &len);
        reply_int(fd, (int64_t)len); return;
    }
    if (strcasecmp(cmd, "RPUSH") == 0 && argc >= 3) {
        size_t len;
        for (int i=2; i<argc; ++i)
            kv_rpush(g_db, args[1], args[i], &len);
        reply_int(fd, (int64_t)len); return;
    }
    if (strcasecmp(cmd, "LPOP") == 0 && argc >= 2) {
        char *v = kv_lpop(g_db, args[1]);
        reply_bulk(fd, v); free(v); return;
    }
    if (strcasecmp(cmd, "RPOP") == 0 && argc >= 2) {
        char *v = kv_rpop(g_db, args[1]);
        reply_bulk(fd, v); free(v); return;
    }
    if (strcasecmp(cmd, "LLEN") == 0 && argc >= 2) {
        size_t l;
        if (kv_llen(g_db, args[1], &l) == 0) reply_int(fd, (int64_t)l);
        else reply_int(fd, 0);
        return;
    }
    if (strcasecmp(cmd, "LINDEX") == 0 && argc >= 3) {
        char *v = kv_lindex(g_db, args[1], arg_i(args, 2));
        reply_bulk(fd, v); free(v); return;
    }
    if (strcasecmp(cmd, "LSET") == 0 && argc >= 4) {
        if (kv_lset(g_db, args[1], arg_i(args,2), args[3]) == 0)
            reply_ok(fd);
        else reply_err(fd, "ERR index out of range");
        return;
    }
    if (strcasecmp(cmd, "LREM") == 0 && argc >= 4) {
        size_t rm;
        kv_lrem(g_db, args[1], arg_i(args,2), args[3], &rm);
        reply_int(fd, (int64_t)rm); return;
    }
    if (strcasecmp(cmd, "RPOPLPUSH") == 0 && argc >= 3) {
        char *v = kv_lpoprpush(g_db, args[1], args[2]);
        reply_bulk(fd, v); free(v); return;
    }

    /* ---- SET ---- */
    if (strcasecmp(cmd, "SADD") == 0 && argc >= 3) {
        int added = 0;
        const char *m[argc-2];
        for (int i=2; i<argc; ++i) m[i-2] = args[i];
        kv_sadd(g_db, args[1], m, argc-2, &added);
        reply_int(fd, added); return;
    }
    if (strcasecmp(cmd, "SREM") == 0 && argc >= 3) {
        int removed = 0;
        const char *m[argc-2];
        for (int i=2; i<argc; ++i) m[i-2] = args[i];
        kv_srem(g_db, args[1], m, argc-2, &removed);
        reply_int(fd, removed); return;
    }
    if (strcasecmp(cmd, "SISMEMBER") == 0 && argc >= 3) {
        reply_int(fd, kv_sismember(g_db, args[1], args[2])); return;
    }
    if (strcasecmp(cmd, "SCARD") == 0 && argc >= 2) {
        size_t c; kv_scard(g_db, args[1], &c);
        reply_int(fd, (int64_t)c); return;
    }
    if (strcasecmp(cmd, "SMEMBERS") == 0 && argc >= 2) {
        char **m=NULL; size_t n=0;
        if (kv_smembers(g_db, args[1], &m, &n) == 0) {
            send_array_len(fd, n);
            for (size_t i=0; i<n; ++i) { reply_bulk(fd, m[i]); free(m[i]); }
            free(m);
        } else reply_err(fd, "ERR");
        return;
    }

    /* ---- HASH ---- */
    if (strcasecmp(cmd, "HSET") == 0 && argc >= 4) {
        int created;
        kv_hset(g_db, args[1], args[2], args[3], &created);
        reply_int(fd, created); return;
    }
    if (strcasecmp(cmd, "HGET") == 0 && argc >= 3) {
        char *v = kv_hget(g_db, args[1], args[2]);
        reply_bulk(fd, v); free(v); return;
    }
    if (strcasecmp(cmd, "HDEL") == 0 && argc >= 3) {
        int removed = 0;
        const char *f[argc-2];
        for (int i=2; i<argc; ++i) f[i-2] = args[i];
        kv_hdel(g_db, args[1], f, argc-2, &removed);
        reply_int(fd, removed); return;
    }
    if (strcasecmp(cmd, "HEXISTS") == 0 && argc >= 3) {
        reply_int(fd, kv_hexists(g_db, args[1], args[2])); return;
    }
    if (strcasecmp(cmd, "HLEN") == 0 && argc >= 2) {
        size_t l; kv_hlen(g_db, args[1], &l);
        reply_int(fd, (int64_t)l); return;
    }
    if (strcasecmp(cmd, "HKEYS") == 0 && argc >= 2) {
        char **m=NULL; size_t n=0;
        if (kv_hkeys(g_db, args[1], &m, &n) == 0) {
            send_array_len(fd, n);
            for (size_t i=0; i<n; ++i) { reply_bulk(fd, m[i]); free(m[i]); }
            free(m);
        } else reply_err(fd, "ERR");
        return;
    }
    if (strcasecmp(cmd, "HVALS") == 0 && argc >= 2) {
        char **m=NULL; size_t n=0;
        if (kv_hvals(g_db, args[1], &m, &n) == 0) {
            send_array_len(fd, n);
            for (size_t i=0; i<n; ++i) { reply_bulk(fd,m[i]); free(m[i]); }
            free(m);
        } else reply_err(fd, "ERR");
        return;
    }
    if (strcasecmp(cmd, "HGETALL") == 0 && argc >= 2) {
        char **m=NULL; size_t n=0;
        if (kv_hgetall(g_db, args[1], &m, &n) == 0) {
            send_array_len(fd, n);
            for (size_t i=0; i<n; ++i) { reply_bulk(fd,m[i]); free(m[i]); }
            free(m);
        } else reply_err(fd, "ERR");
        return;
    }

    /* ---- SORTED SET ---- */
    if (strcasecmp(cmd, "ZADD") == 0 && argc >= 4) {
        int added = 0;
        for (int i=2; i+1 < argc; i+=2) {
            double score = strtod(args[i], NULL);
            kv_zadd(g_db, args[1], score, args[i+1], &added);
        }
        reply_int(fd, added); return;
    }
    if (strcasecmp(cmd, "ZREM") == 0 && argc >= 3) {
        int removed = 0;
        for (int i=2; i<argc; ++i)
            kv_zrem(g_db, args[1], args[i], &removed);
        reply_int(fd, removed); return;
    }
    if (strcasecmp(cmd, "ZSCORE") == 0 && argc >= 3) {
        int found;
        double sc = kv_zscore(g_db, args[1], args[2], &found);
        if (found) {
            char buf[32];
            snprintf(buf,sizeof(buf),"%g",sc);
            reply_bulk(fd, buf);
        } else reply_nil(fd);
        return;
    }
    if (strcasecmp(cmd, "ZCARD") == 0 && argc >= 2) {
        size_t c; kv_zcard(g_db, args[1], &c);
        reply_int(fd, (int64_t)c); return;
    }
    if (strcasecmp(cmd, "ZRANK") == 0 && argc >= 3) {
        int64_t r; kv_zrank(g_db, args[1], args[2], &r);
        reply_int(fd, r); return;
    }
    if (strcasecmp(cmd, "ZREVRANK") == 0 && argc >= 3) {
        int64_t r; kv_zrevrank(g_db, args[1], args[2], &r);
        reply_int(fd, r); return;
    }
    if (strcasecmp(cmd, "ZRANGE") == 0 && argc >= 4) {
        char **m=NULL; size_t n=0;
        kv_zrange(g_db, args[1], arg_i(args,2), arg_i(args,3), &m, &n);
        send_array_len(fd, n);
        for (size_t i=0; i<n; ++i) { reply_bulk(fd,m[i]); free(m[i]); }
        free(m); return;
    }
    if (strcasecmp(cmd, "ZREVRANGE") == 0 && argc >= 4) {
        char **m=NULL; size_t n=0;
        kv_zrevrange(g_db, args[1], arg_i(args,2), arg_i(args,3), &m, &n);
        send_array_len(fd, n);
        for (size_t i=0; i<n; ++i) { reply_bulk(fd,m[i]); free(m[i]); }
        free(m); return;
    }

    /* ---- PUB/SUB ---- */
    if (strcasecmp(cmd, "PUBLISH") == 0 && argc >= 3) {
        const char *ch = args[1];
        char msg[BULK_MSG_MAX]; msg[0]=0;
        for (int i=2; i<argc; ++i) {
            if (i>2) strncat(msg," ",sizeof(msg)-strlen(msg)-1);
            strncat(msg, args[i], sizeof(msg)-strlen(msg)-1);
        }
        int c = pubsub_publish(g_ps, ch, msg);
        reply_int(fd, c); return;
    }
    /* SUBSCRIBE / UNSUBSCRIBE are handled in the client loop (repl) */

    reply_err(fd, "ERR unknown command");
}

/* ================================================================== */
/*  Client handler (runs inside thread pool)                           */
/* ================================================================== */
static void handle_client_unlocked(int fd) {
    char rxbuf[BUFFER_SIZE * 4];
    size_t rxlen = 0;
    char line[BUFFER_SIZE];
    int quit = 0;
    int subscribed = 0;        /* we are in Pub/Sub mode? */

    pthread_mutex_t wlock = PTHREAD_MUTEX_INITIALIZER;

    while (!quit) {
        ssize_t n = read(fd, rxbuf + rxlen, sizeof(rxbuf) - rxlen - 1);
        if (n <= 0) break;
        rxlen += (size_t)n;
        rxbuf[rxlen] = '\0';

        char *p = rxbuf;
        char *nl;
        while ((nl = strchr(p, '\n')) != NULL) {
            size_t linelen = (size_t)(nl - p);
            if (linelen >= sizeof(line)) linelen = sizeof(line) - 1;
            memcpy(line, p, linelen);
            line[linelen] = '\0';

            /* quick parse to check for QUIT / SUBSCRIBE / UNSUBSCRIBE */
            char tmp[MAX_ARGS]; strncpy(tmp, line, sizeof(tmp)-1);
            tmp[sizeof(tmp)-1] = '\0';
            char *qa[MAX_ARGS];
            int qc = parse_line(tmp, qa, MAX_ARGS);

            if (qc >= 1) {
                const char *qc0 = qa[0];

                if (strcasecmp(qc0, "SUBSCRIBE") == 0 && qc >= 2) {
                    for (int i=1; i<qc; ++i) {
                        pubsub_subscribe(g_ps, fd, &wlock, qa[i]);
                        /* Reply: array [subscribe, channel, numsub] */
                        char buf[512];
                        int ns = pubsub_numsub(g_ps, qa[i]);
                        int len = snprintf(buf, sizeof(buf),
                            "*3\r\n$9\r\nsubscribe\r\n$%zu\r\n%s\r\n:%d\r\n",
                            strlen(qa[i]), qa[i], ns);
                        pthread_mutex_lock(&wlock);
                        send_raw(fd, buf, (size_t)len);
                        pthread_mutex_unlock(&wlock);
                    }
                    subscribed = 1;
                    p = nl + 1; continue;
                }
                if (strcasecmp(qc0, "UNSUBSCRIBE") == 0) {
                    if (qc == 1) {
                        pubsub_unsubscribe_all(g_ps, fd);
                        char buf[128];
                        int len = snprintf(buf, sizeof(buf),
                            "*3\r\n$11\r\nunsubscribe\r\n$0\r\n\r\n:0\r\n");
                        pthread_mutex_lock(&wlock);
                        send_raw(fd, buf, (size_t)len);
                        pthread_mutex_unlock(&wlock);
                    } else {
                        for (int i=1; i<qc; ++i) {
                            pubsub_unsubscribe(g_ps, fd, qa[i]);
                            int ns = pubsub_numsub(g_ps, qa[i]);
                            char buf[512];
                            int len = snprintf(buf, sizeof(buf),
                                "*3\r\n$11\r\nunsubscribe\r\n$%zu\r\n%s\r\n:%d\r\n",
                                strlen(qa[i]), qa[i], ns);
                            pthread_mutex_lock(&wlock);
                            send_raw(fd, buf, (size_t)len);
                            pthread_mutex_unlock(&wlock);
                        }
                    }
                    p = nl + 1; continue;
                }
                if (subscribed) {
                    /* In Pub/Sub mode – ignore everything except SUB/UNSUB */
                    p = nl + 1; continue;
                }

                if (strcasecmp(qc0, "QUIT") == 0) {
                    pthread_mutex_lock(&wlock);
                    send_raw(fd, "+OK\r\n", 5);
                    pthread_mutex_unlock(&wlock);
                    quit = 1; break;
                }

                /* normal command */
                pthread_mutex_lock(&wlock);
                process_command(fd, line);
                pthread_mutex_unlock(&wlock);
            }
            p = nl + 1;
        }

        size_t remaining = (size_t)((rxbuf + rxlen) - p);
        if (remaining >= sizeof(rxbuf)) remaining = 0;
        memmove(rxbuf, p, remaining);
        rxlen = remaining;
    }
    pubsub_unsubscribe_all(g_ps, fd);
    close(fd);
}

/* ================================================================== */
/*  Thread Pool workers                                               */
/* ================================================================== */
static void *tp_worker(void *arg) {
    (void)arg;
    while (1) {
        pthread_mutex_lock(&g_tp.lock);
        while (!g_tp.stop && g_tp.head == NULL)
            pthread_cond_wait(&g_tp.cond, &g_tp.lock);
        if (g_tp.stop && g_tp.head == NULL) {
            pthread_mutex_unlock(&g_tp.lock); break;
        }
        tp_job_t *j = g_tp.head;
        g_tp.head = j->next;
        if (!g_tp.head) g_tp.tail = NULL;
        pthread_mutex_unlock(&g_tp.lock);

        handle_client_unlocked(j->fd);
        free(j);
    }
    return NULL;
}

static void tp_init(void) {
    pthread_mutex_init(&g_tp.lock, NULL);
    pthread_cond_init(&g_tp.cond, NULL);
    g_tp.head = g_tp.tail = NULL;
    g_tp.stop = 0;
    for (int i = 0; i < THREAD_POOL_SIZE; ++i)
        pthread_create(&g_tp.workers[i], NULL, tp_worker, NULL);
}

static void tp_submit(int fd) {
    tp_job_t *j = malloc(sizeof(*j));
    j->fd = fd; j->next = NULL;
    pthread_mutex_lock(&g_tp.lock);
    if (!g_tp.tail) g_tp.head = g_tp.tail = j;
    else { g_tp.tail->next = j; g_tp.tail = j; }
    pthread_cond_signal(&g_tp.cond);
    pthread_mutex_unlock(&g_tp.lock);
}

static void tp_shutdown(void) {
    pthread_mutex_lock(&g_tp.lock);
    g_tp.stop = 1;
    pthread_cond_broadcast(&g_tp.cond);
    pthread_mutex_unlock(&g_tp.lock);
    for (int i = 0; i < THREAD_POOL_SIZE; ++i)
        pthread_join(g_tp.workers[i], NULL);
    pthread_mutex_destroy(&g_tp.lock);
    pthread_cond_destroy(&g_tp.cond);
}

/* ---- expiry thread ---- */
static void *expire_thread(void *arg) {
    (void)arg;
    while (g_running) {
        usleep(EXPIRE_INTERVAL_MS * 1000);
        struct timeval tv; gettimeofday(&tv, NULL);
        int64_t now = (int64_t)tv.tv_sec * 1000LL + (int64_t)tv.tv_usec / 1000LL;
        kv_active_expire(g_db, 20, now);
    }
    return NULL;
}

/* ================================================================== */
/*  Main                                                               */
/* ================================================================== */
int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    const char *path = NULL;
#ifdef CLUSTER_ENABLED
    const char *cluster_conf_path = NULL;
    int cluster_bus_port = 0;
#endif

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc)
            path = argv[++i];
#ifdef CLUSTER_ENABLED
        else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
            cluster_conf_path = argv[++i];
        else if (strcmp(argv[i], "-C") == 0 && i + 1 < argc)
            cluster_bus_port = atoi(argv[++i]);
#endif
    }

    g_db = kv_open(path);
    if (!g_db) { fprintf(stderr, "Cannot open DB\n"); return 1; }

    g_ps = pubsub_create();
    if (!g_ps) { kv_close(g_db); return 1; }

    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);
    signal(SIGPIPE, SIG_IGN);

#ifdef CLUSTER_ENABLED
    if (cluster_conf_path) {
        cluster_conf_t *cfg = cluster_conf_load(
            cluster_conf_path, port,
            cluster_bus_port ? cluster_bus_port : port + 10000);
        if (!cfg) {
            fprintf(stderr, "Errore cluster config\n");
            pubsub_destroy(g_ps); kv_close(g_db); return 1;
        }
        const char *my_id = "";
        if (cfg->my_node >= 0 && (size_t)cfg->my_node < cfg->node_count)
            my_id = cfg->nodes[cfg->my_node].id;
        load_or_create_nodeid_from_config(
            path ? path : "./.nodeid", port, my_id);
        g_cs = cluster_init(cfg, g_db);
        if (!g_cs) {
            cluster_conf_free(cfg);
            pubsub_destroy(g_ps); kv_close(g_db); return 1;
        }
        printf("Cluster enabled. My ID: %s\n", g_my_id);
    }
#endif

    tp_init();

    pthread_t exp_tid;
    pthread_create(&exp_tid, NULL, expire_thread, NULL);
    pthread_detach(exp_tid);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef __APPLE__
    setsockopt(srv, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int retries = 5;
    while (bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0 && retries > 0) {
        if (errno == EADDRINUSE) {
            fprintf(stderr,"bind port %d: in use, retrying...\n", port);
            sleep(1); retries--; continue;
        }
        perror("bind"); return 1;
    }
    if (retries == 0) { fprintf(stderr,"Could not bind port %d\n",port); return 1; }
    listen(srv, 128);
    printf("antzkv-server listening on port %d (thread pool %d workers)\n",
           port, THREAD_POOL_SIZE);

    while (g_running) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int client = accept(srv, (struct sockaddr*)&cli_addr, &cli_len);
        if (client < 0) {
            if (errno == EINTR) continue;
            perror("accept"); continue;
        }
        tp_submit(client);
    }

    close(srv);
    tp_shutdown();
#ifdef CLUSTER_ENABLED
    if (g_cs) cluster_shutdown(g_cs);
#endif
    pubsub_destroy(g_ps);
    kv_close(g_db);
    return 0;
}
