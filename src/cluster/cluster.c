#include "cluster.h"
#include "conf.h"
#include "kvdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/time.h>
#include <fcntl.h>
#include <ctype.h>

static uint64_t msec_now(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000ULL + (uint64_t)tv.tv_usec / 1000ULL;
}

static int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void cluster_send_msg(cluster_peer_t *peer, const char *msg) {
    if (peer->fd < 0 || !peer->alive) return;
    size_t len = strlen(msg);
    if (write(peer->fd, msg, len) < (ssize_t)len) {
        pthread_mutex_lock(&peer->lock);
        peer->alive = 0;
        close(peer->fd);
        peer->fd = -1;
        pthread_mutex_unlock(&peer->lock);
    }
}

static cluster_peer_t *find_peer(cluster_state_t *cs, const char *id) {
    cluster_peer_t *p = cs->peers;
    while (p) {
        if (strcmp(p->info.id, id) == 0) return p;
        p = p->next;
    }
    return NULL;
}

static void add_peer(cluster_state_t *cs, cluster_peer_t *peer) {
    pthread_rwlock_wrlock(&cs->peers_lock);
    peer->next = cs->peers;
    cs->peers = peer;
    cs->peer_count++;
    pthread_rwlock_unlock(&cs->peers_lock);
}

static int connect_peer(cluster_state_t *cs, cluster_peer_t *peer) {
    if (peer->fd >= 0) return 0;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer->info.cluster_port);
    if (inet_pton(AF_INET, peer->info.host, &addr.sin_addr) <= 0) {
        close(fd); return -1;
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }
    set_nonblock(fd);
    peer->fd = fd;
    peer->last_seen = msec_now();
    peer->alive = 1;
    /* invio JOIN */
    char msg[256];
    int cp = (cs->conf && cs->conf->my_node >= 0 && (size_t)cs->conf->my_node < cs->conf->node_count)
             ? cs->conf->nodes[cs->conf->my_node].client_port : 0;
    snprintf(msg, sizeof(msg), "JOIN %s %d\n", cs->my_id, cp);
    (void)write(fd, msg, strlen(msg));
    return 0;
}

/* ----- protocol helpers ----- */
cluster_cmd_t cluster_parse_cmd(const char *line, char *rest, size_t rest_len) {
    const char *p = line;
    while (*p && isspace((unsigned char)*p)) ++p;
    if (strncasecmp(p, "HEARTBEAT", 9) == 0) { if (rest) strncpy(rest, p+9, rest_len); return CMD_HEARTBEAT; }
    if (strncasecmp(p, "SET ", 4) == 0)     { if (rest) strncpy(rest, p+4, rest_len); return CMD_SET; }
    if (strncasecmp(p, "DEL ", 4) == 0)     { if (rest) strncpy(rest, p+4, rest_len); return CMD_DEL; }
    if (strncasecmp(p, "JOIN ", 5) == 0)    { if (rest) strncpy(rest, p+5, rest_len); return CMD_JOIN; }
    if (strncasecmp(p, "SYNC_REQ", 8) == 0) { if (rest) strncpy(rest, p+8, rest_len); return CMD_SYNC_REQ; }
    if (strncasecmp(p, "SNAPSHOT ", 9) == 0){ if (rest) strncpy(rest, p+9, rest_len); return CMD_SNAPSHOT; }
    if (strncasecmp(p, "SNAPSHOT_END", 12)==0){ if (rest) strncpy(rest, p+12, rest_len); return CMD_SNAPSHOT_END; }
    return CMD_UNKNOWN;
}

static int parse_meta(const char *rest, kv_meta_t *meta, char *key, char *val) {
    return sscanf(rest, "%15s %llu %llu %4095s %4095s",
                  meta->origin,
                  (unsigned long long*)&meta->version,
                  (unsigned long long*)&meta->wallclock,
                  key, val) >= 3;
}

static int parse_join(const char *rest, char *id, size_t id_len, int *client_port) {
    char fmt[64];
    snprintf(fmt, sizeof(fmt), "%%%zus %%d", id_len-1);
    return sscanf(rest, fmt, id, client_port) == 2;
}

int cluster_build_msg(char *buf, size_t len, cluster_cmd_t cmd, const char *key, const char *value, const kv_meta_t *meta) {
    switch (cmd) {
        case CMD_HEARTBEAT:
            return snprintf(buf, len, "HEARTBEAT %llu\n", (unsigned long long)(meta ? meta->version : 0));
        case CMD_SET:
            return snprintf(buf, len, "SET %s %llu %llu %s %s\n",
                            meta->origin, (unsigned long long)meta->version,
                            (unsigned long long)meta->wallclock, key, value);
        case CMD_DEL:
            return snprintf(buf, len, "DEL %s %llu %llu %s\n",
                            meta->origin, (unsigned long long)meta->version,
                            (unsigned long long)meta->wallclock, key);
        case CMD_SYNC_REQ:
            return snprintf(buf, len, "SYNC_REQ %s\n", key);
        case CMD_SNAPSHOT:
            return snprintf(buf, len, "SNAPSHOT %llu\n", (unsigned long long)(meta ? meta->version : 0));
        case CMD_SNAPSHOT_END:
            return snprintf(buf, len, "SNAPSHOT_END\n");
        default:
            return -1;
    }
}

/* ---- thread: replication daemon ---- */
static void *repl_thread(void *arg) {
    cluster_state_t *cs = (cluster_state_t *)arg;
    pthread_mutex_lock(&cs->repl_lock);
    while (!cs->stop) {
        if (cs->repl_head == cs->repl_tail) {
            pthread_cond_wait(&cs->repl_cond, &cs->repl_lock);
            continue;
        }
        size_t idx = cs->repl_tail % cs->repl_cap;
        char *item = cs->repl_buf[idx];
        cs->repl_tail++;
        pthread_mutex_unlock(&cs->repl_lock);

        pthread_rwlock_rdlock(&cs->peers_lock);
        cluster_peer_t *p = cs->peers;
        while (p) {
            cluster_send_msg(p, item);
            p = p->next;
        }
        pthread_rwlock_unlock(&cs->peers_lock);
        free(item);
        pthread_mutex_lock(&cs->repl_lock);
    }
    pthread_mutex_unlock(&cs->repl_lock);
    return NULL;
}

/* ---- thread: heartbeat daemon ---- */
static void *hb_thread(void *arg) {
    cluster_state_t *cs = (cluster_state_t *)arg;
    char msg[256];
    while (!cs->stop) {
        usleep(CLUSTER_HEARTBEAT_MS * 1000);
        uint64_t v = kv_db_version(cs->db);
        kv_meta_t meta = {v, 0, ""};
        cluster_build_msg(msg, sizeof(msg), CMD_HEARTBEAT, NULL, NULL, &meta);

        pthread_rwlock_rdlock(&cs->peers_lock);
        cluster_peer_t *p = cs->peers;
        while (p) {
            if (p->fd < 0 || !p->alive) {
                connect_peer(cs, p);
            }
            if (p->fd >= 0 && p->alive)
                cluster_send_msg(p, msg);
            p = p->next;
        }
        pthread_rwlock_unlock(&cs->peers_lock);

        /* cleanup dead peers */
        uint64_t now = msec_now();
        pthread_rwlock_wrlock(&cs->peers_lock);
        p = cs->peers;
        while (p) {
            if (p->alive && now - p->last_seen > CLUSTER_DEAD_MS) {
                p->alive = 0;
                if (p->fd >= 0) { close(p->fd); p->fd = -1; }
            }
            p = p->next;
        }
        pthread_rwlock_unlock(&cs->peers_lock);
    }
    return NULL;
}

/* ---- incoming cluster connection handler wrapper ---- */
typedef struct {
    cluster_state_t *cs;
    int fd;
} handle_args_t;

static void *handle_client(void *varg);

/* ---- thread: cluster bus listener ---- */
static void *bus_listener_thread(void *arg) {
    cluster_state_t *cs = (cluster_state_t *)arg;
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("cluster socket"); return NULL; }
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(cs->bus_port);
    if (bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("cluster bind"); close(srv); return NULL;
    }
    listen(srv, 16);
    cs->listener_fd = srv;

    while (!cs->stop) {
        struct sockaddr_in cli;
        socklen_t clilen = sizeof(cli);
        int fd = accept(srv, (struct sockaddr*)&cli, &clilen);
        if (fd < 0) {
            if (errno == EINTR) continue;
            continue;
        }
        set_nonblock(fd);

        pthread_t tid;
        handle_args_t *ha = malloc(sizeof(*ha));
        ha->cs = cs;
        ha->fd = fd;
        pthread_create(&tid, NULL, handle_client, ha);
        pthread_detach(tid);
    }
    close(srv);
    cs->listener_fd = -1;
    return NULL;
}

static void *handle_client(void *varg) {
    handle_args_t *ha = (handle_args_t *)varg;
    cluster_state_t *cs = ha->cs;
    int fd = ha->fd;
    free(ha);

    char rx[4096];
    size_t rxlen = 0;
    char line[4096];

    while (!cs->stop) {
        ssize_t n = read(fd, rx + rxlen, sizeof(rx) - rxlen - 1);
        if (n <= 0) {
            if (n < 0 && errno == EAGAIN) { usleep(10000); continue; }
            break;
        }
        rxlen += n;
        rx[rxlen] = '\0';

        char *p = rx;
        char *nl;
        while ((nl = strchr(p, '\n')) != NULL) {
            size_t linelen = nl - p;
            if (linelen >= sizeof(line)) linelen = sizeof(line) - 1;
            memcpy(line, p, linelen);
            line[linelen] = '\0';

            cluster_cmd_t cmd;
            char rest[4096];
            cmd = cluster_parse_cmd(line, rest, sizeof(rest));

            if (cmd == CMD_HEARTBEAT) {
                unsigned long long v;
                if (sscanf(rest, "%llu", &v) == 1) {
                    pthread_rwlock_wrlock(&cs->peers_lock);
                    cluster_peer_t *peer = cs->peers;
                    while (peer) {
                        if (peer->fd == fd || peer->fd < 0) {
                            peer->last_seen = msec_now();
                            if (peer->fd < 0) peer->fd = fd;
                            peer->last_version = v;
                            break;
                        }
                        peer = peer->next;
                    }
                    pthread_rwlock_unlock(&cs->peers_lock);
                }
            } else if (cmd == CMD_SET) {
                kv_meta_t meta;
                char key[4096] = {0}, value[4096] = {0};
                if (parse_meta(rest, &meta, key, value)) {
                    kv_set_meta(cs->db, key, value, &meta);
                }
            } else if (cmd == CMD_DEL) {
                kv_meta_t meta;
                char key[4096] = {0};
                if (parse_meta(rest, &meta, key, NULL)) {
                    kv_del_meta(cs->db, key, &meta);
                }
            } else if (cmd == CMD_JOIN) {
                char id[16] = {0};
                int client_port;
                if (parse_join(rest, id, sizeof(id), &client_port)) {
                    pthread_rwlock_wrlock(&cs->peers_lock);
                    cluster_peer_t *peer = find_peer(cs, id);
                    if (peer) {
                        peer->fd = fd;
                        peer->last_seen = msec_now();
                        peer->alive = 1;
                    }
                    pthread_rwlock_unlock(&cs->peers_lock);
                }
            } else if (cmd == CMD_SYNC_REQ) {
                char **keys = NULL;
                size_t kcount = 0;
                if (kv_keys(cs->db, &keys, &kcount) == 0) {
                    char msg[CLUSTER_MSG_SIZE];
                    uint64_t ver = kv_db_version(cs->db);
                    kv_meta_t meta = {ver, 0, ""};
                    cluster_build_msg(msg, sizeof(msg), CMD_SNAPSHOT, NULL, NULL, &meta);
                    write(fd, msg, strlen(msg));

                    for (size_t i = 0; i < kcount && !cs->stop; ++i) {
                        kv_meta_t m;
                        char *v = kv_get_meta(cs->db, keys[i], &m);
                        if (v) {
                            cluster_build_msg(msg, sizeof(msg), CMD_SET, keys[i], v, &m);
                            write(fd, msg, strlen(msg));
                            free(v);
                        }
                    }
                    kv_keys_free(keys, kcount);
                    cluster_build_msg(msg, sizeof(msg), CMD_SNAPSHOT_END, NULL, NULL, NULL);
                    write(fd, msg, strlen(msg));
                }
            }
            p = nl + 1;
        }
        size_t remaining = rx + rxlen - p;
        memmove(rx, p, remaining);
        rxlen = remaining;
    }
    close(fd);
    return NULL;
}

/* ---- public ---- */
cluster_state_t *cluster_init(cluster_conf_t *conf, kv_table_t *db) {
    cluster_state_t *cs = calloc(1, sizeof(*cs));
    if (!cs) return NULL;
    cs->db = db;
    cs->conf = conf;
    cs->bus_port = conf->my_cluster_port ? conf->my_cluster_port : 16380;
    if (conf->my_node >= 0 && (size_t)conf->my_node < conf->node_count) {
        strncpy(cs->my_id, conf->nodes[conf->my_node].id, NODE_ID_LEN - 1);
        cs->my_id[NODE_ID_LEN - 1] = '\0';
    }
    cs->stop = 0;
    pthread_rwlock_init(&cs->peers_lock, NULL);
    pthread_mutex_init(&cs->repl_lock, NULL);
    pthread_cond_init(&cs->repl_cond, NULL);

    for (size_t i = 0; i < conf->node_count; ++i) {
        if ((int)i == conf->my_node) continue;
        cluster_peer_t *peer = calloc(1, sizeof(*peer));
        if (!peer) continue;
        peer->info = conf->nodes[i];
        peer->fd = -1;
        peer->alive = 0;
        pthread_mutex_init(&peer->lock, NULL);
        add_peer(cs, peer);
    }

    cs->repl_cap = 1024;
    cs->repl_buf = calloc(cs->repl_cap, sizeof(char*));
    if (!cs->repl_buf) { free(cs); return NULL; }
    cs->repl_head = cs->repl_tail = 0;
    cs->repl_running = 1;

    pthread_create(&cs->bus_listener, NULL, bus_listener_thread, cs);
    pthread_create(&cs->repl_daemon, NULL, repl_thread, cs);
    pthread_create(&cs->heartbeat_daemon, NULL, hb_thread, cs);

    pthread_rwlock_rdlock(&cs->peers_lock);
    cluster_peer_t *p = cs->peers;
    while (p) {
        connect_peer(cs, p);
        p = p->next;
    }
    pthread_rwlock_unlock(&cs->peers_lock);

    return cs;
}

void cluster_shutdown(cluster_state_t *cs) {
    if (!cs) return;
    cs->stop = 1;
    pthread_cond_broadcast(&cs->repl_cond);
    pthread_join(cs->repl_daemon, NULL);
    pthread_join(cs->heartbeat_daemon, NULL);
    if (cs->listener_fd >= 0) {
        close(cs->listener_fd);
    }
    pthread_join(cs->bus_listener, NULL);

    cluster_peer_t *p = cs->peers;
    while (p) {
        cluster_peer_t *n = p->next;
        if (p->fd >= 0) close(p->fd);
        pthread_mutex_destroy(&p->lock);
        free(p);
        p = n;
    }

    for (size_t i = cs->repl_tail; i < cs->repl_head; ++i) {
        free(cs->repl_buf[i % cs->repl_cap]);
    }
    free(cs->repl_buf);
    pthread_rwlock_destroy(&cs->peers_lock);
    pthread_mutex_destroy(&cs->repl_lock);
    pthread_cond_destroy(&cs->repl_cond);
    free(cs);
}

static void repl_enqueue(cluster_state_t *cs, const char *msg) {
    pthread_mutex_lock(&cs->repl_lock);
    size_t next = (cs->repl_head + 1) % cs->repl_cap;
    if (next == cs->repl_tail) {
        free(cs->repl_buf[cs->repl_tail % cs->repl_cap]);
        cs->repl_tail = (cs->repl_tail + 1) % cs->repl_cap;
    }
    cs->repl_buf[cs->repl_head % cs->repl_cap] = strdup(msg);
    cs->repl_head = next;
    pthread_cond_signal(&cs->repl_cond);
    pthread_mutex_unlock(&cs->repl_lock);
}

int cluster_replicate_set(cluster_state_t *cs, const char *key, const char *value, const kv_meta_t *meta) {
    if (!cs) return -1;
    char msg[CLUSTER_MSG_SIZE];
    cluster_build_msg(msg, sizeof(msg), CMD_SET, key, value, meta);
    repl_enqueue(cs, msg);
    return 0;
}

int cluster_replicate_del(cluster_state_t *cs, const char *key, const kv_meta_t *meta) {
    if (!cs) return -1;
    char msg[CLUSTER_MSG_SIZE];
    cluster_build_msg(msg, sizeof(msg), CMD_DEL, key, NULL, meta);
    repl_enqueue(cs, msg);
    return 0;
}
