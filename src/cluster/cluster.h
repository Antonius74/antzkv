#ifndef CLUSTER_H
#define CLUSTER_H

#include "../cluster/conf.h"
#include "kvdb.h"
#include <pthread.h>
#include <stdint.h>

#define CLUSTER_MSG_SIZE 8192
#define CLUSTER_HEARTBEAT_MS 500
#define CLUSTER_DEAD_MS     2000
#define CLUSTER_MAX_PEERS   16

typedef enum {
    CMD_HEARTBEAT,
    CMD_SET,
    CMD_DEL,
    CMD_JOIN,
    CMD_SYNC_REQ,
    CMD_SNAPSHOT,
    CMD_SNAPSHOT_END,
    CMD_UNKNOWN
} cluster_cmd_t;

typedef struct cluster_peer {
    cluster_node_conf_t info;
    int fd;               /* connessione TCP cluster (-1 = non connesso) */
    int incoming;         /* 1 se connessione incoming */
    uint64_t last_seen;   /* msec dall'epoch */
    uint64_t last_version;
    int alive;
    pthread_mutex_t lock;
    struct cluster_peer *next;
} cluster_peer_t;

typedef struct {
    char my_id[NODE_ID_LEN];
    int bus_port;
    cluster_conf_t *conf;
    cluster_peer_t *peers;
    size_t peer_count;
    pthread_rwlock_t peers_lock;

    /* replication queue */
    char **repl_buf;
    size_t repl_head;
    size_t repl_tail;
    size_t repl_cap;
    pthread_mutex_t repl_lock;
    pthread_cond_t repl_cond;
    int repl_running;

    kv_table_t *db;
    int listener_fd;
    int stop;

    /* thread daemon */
    pthread_t bus_listener;
    pthread_t repl_daemon;
    pthread_t heartbeat_daemon;
} cluster_state_t;

cluster_state_t *cluster_init(cluster_conf_t *conf, kv_table_t *db);
void cluster_shutdown(cluster_state_t *cs);

/* inviare una scrittura a tutti i peer */
int cluster_replicate_set(cluster_state_t *cs, const char *key, const char *value, const kv_meta_t *meta);
int cluster_replicate_del(cluster_state_t *cs, const char *key, const kv_meta_t *meta);

/* protocol helpers */
cluster_cmd_t cluster_parse_cmd(const char *line, char *rest, size_t rest_len);
int cluster_build_msg(char *buf, size_t len, cluster_cmd_t cmd, const char *key, const char *value, const kv_meta_t *meta);

#endif
