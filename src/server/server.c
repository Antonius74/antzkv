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

#ifdef CLUSTER_ENABLED
#include "cluster/conf.h"
#include "cluster/cluster.h"
#endif

#define DEFAULT_PORT 6379
#define BUFFER_SIZE  4096
#define MAX_ARGS     4

#ifdef CLUSTER_ENABLED
static cluster_state_t *g_cs = NULL;
static char g_my_id[16] = {0};
static void load_or_create_nodeid_from_config(const char *base, int port, const char *id_from_config) {
    char path[256];
    snprintf(path, sizeof(path), "%s.%d", base, port);
    FILE *fp = fopen(path, "r");
    if (fp) {
        if (fgets(g_my_id, sizeof(g_my_id), fp)) {
            g_my_id[strcspn(g_my_id, "\r\n")] = '\0';
        }
        fclose(fp);
        if (id_from_config && strcmp(g_my_id, id_from_config) != 0) {
            /* config changed, update */
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
        snprintf(g_my_id, sizeof(g_my_id), "n%05x", (int)(tv.tv_usec % 0x100000));
    }
    fp = fopen(path, "w");
    if (fp) { fprintf(fp, "%s\n", g_my_id); fclose(fp); }
}
#endif

static kv_table_t *g_db = NULL;
static int g_running = 1;

static void handle_sigint(int sig) {
    (void)sig;
    g_running = 0;
}

static void send_reply(int fd, const char *msg) {
    write(fd, msg, strlen(msg));
    write(fd, "\n", 1);
}

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

static void process_command(int fd, char *line) {
    char *args[MAX_ARGS];
    int argc = parse_line(line, args, MAX_ARGS);
    if (argc == 0) return;

    if (strcasecmp(args[0], "SET") == 0 && argc >= 3) {
        char *key = args[1];
        char *val = args[2];
        if (kv_set(g_db, key, val) == 0) {
            send_reply(fd, "OK");
#ifdef CLUSTER_ENABLED
            if (g_cs) {
                kv_meta_t meta;
                char *check = kv_get_meta(g_db, key, &meta);
                if (check) {
                    memcpy(meta.origin, g_my_id, 15);
                    meta.origin[15] = '\0';
                    cluster_replicate_set(g_cs, key, val, &meta);
                    free(check);
                }
            }
#endif
        } else {
            send_reply(fd, "ERR");
        }
    } else if (strcasecmp(args[0], "GET") == 0 && argc >= 2) {
        char *val = kv_get(g_db, args[1]);
        if (val) {
            send_reply(fd, val);
            free(val);
        } else {
            send_reply(fd, "(nil)");
        }
    } else if (strcasecmp(args[0], "DEL") == 0 && argc >= 2) {
        int ok = 0;
        for (int i = 1; i < argc; ++i) {
            if (kv_del(g_db, args[i]) == 0) ok++;
#ifdef CLUSTER_ENABLED
            if (g_cs && ok) {
                uint64_t v = kv_next_version(g_db);
                struct timeval tv;
                gettimeofday(&tv, NULL);
                uint64_t wc = (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
                kv_meta_t meta = {v, wc, ""};
                memcpy(meta.origin, g_my_id, 15); meta.origin[15] = '\0';
                cluster_replicate_del(g_cs, args[i], &meta);
            }
#endif
        }
        char out[64];
        snprintf(out, sizeof(out), "%d", ok);
        send_reply(fd, out);
    } else if (strcasecmp(args[0], "EXISTS") == 0 && argc >= 2) {
        int n = 0;
        for (int i = 1; i < argc; ++i)
            if (kv_exists(g_db, args[i])) n++;
        char out[64];
        snprintf(out, sizeof(out), "%d", n);
        send_reply(fd, out);
    } else if (strcasecmp(args[0], "KEYS") == 0) {
        char **keys = NULL;
        size_t kcount = 0;
        if (kv_keys(g_db, &keys, &kcount) == 0) {
            if (kcount == 0)
                send_reply(fd, "(empty)");
            else {
                char linebuf[BUFFER_SIZE];
                linebuf[0] = '\0';
                for (size_t i = 0; i < kcount; ++i) {
                    if (i > 0) strncat(linebuf, " ", sizeof(linebuf) - strlen(linebuf) - 1);
                    strncat(linebuf, keys[i], sizeof(linebuf) - strlen(linebuf) - 1);
                }
                send_reply(fd, linebuf);
            }
            kv_keys_free(keys, kcount);
        } else {
            send_reply(fd, "ERR");
        }
    } else if (strcasecmp(args[0], "SAVE") == 0) {
        if (kv_save(g_db) == 0)
            send_reply(fd, "OK");
        else
            send_reply(fd, "ERR");
    } else if (strcasecmp(args[0], "PING") == 0) {
        send_reply(fd, "PONG");
    } else if (strcasecmp(args[0], "QUIT") == 0) {
        send_reply(fd, "OK");
    } else {
        send_reply(fd, "ERR unknown command");
    }
}

typedef struct {
    int fd;
} client_args_t;

static void *client_thread(void *arg) {
    client_args_t *args = (client_args_t *)arg;
    int fd = args->fd;
    free(args);

    char rxbuf[BUFFER_SIZE * 2];
    size_t rxlen = 0;
    char line[BUFFER_SIZE];
    int quit = 0;

    while (!quit) {
        ssize_t n = read(fd, rxbuf + rxlen, sizeof(rxbuf) - rxlen - 1);
        if (n <= 0) break;
        rxlen += n;
        rxbuf[rxlen] = '\0';

        char *p = rxbuf;
        char *nl;
        while ((nl = strchr(p, '\n')) != NULL) {
            size_t linelen = nl - p;
            if (linelen >= sizeof(line)) linelen = sizeof(line) - 1;
            memcpy(line, p, linelen);
            line[linelen] = '\0';

            process_command(fd, line);

            {
                char *tmp_args[MAX_ARGS];
                char tmp_line[BUFFER_SIZE];
                strncpy(tmp_line, line, sizeof(tmp_line)-1);
                tmp_line[sizeof(tmp_line)-1] = '\0';
                if (parse_line(tmp_line, tmp_args, MAX_ARGS) >= 1 &&
                    strcasecmp(tmp_args[0], "QUIT") == 0) {
                    quit = 1;
                    break;
                }
            }

            p = nl + 1;
        }

        size_t remaining = rxbuf + rxlen - p;
        if (remaining >= sizeof(rxbuf)) remaining = 0;
        memmove(rxbuf, p, remaining);
        rxlen = remaining;
    }
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    const char *path = NULL;
#ifdef CLUSTER_ENABLED
    const char *cluster_conf_path = NULL;
    int cluster_bus_port = 0;
#endif

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc) {
            path = argv[++i];
        }
#ifdef CLUSTER_ENABLED
        else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            cluster_conf_path = argv[++i];
        } else if (strcmp(argv[i], "-C") == 0 && i + 1 < argc) {
            cluster_bus_port = atoi(argv[++i]);
        }
#endif
    }

    g_db = kv_open(path);
    if (!g_db) {
        fprintf(stderr, "Impossibile aprire il DB\n");
        return 1;
    }

    signal(SIGINT, handle_sigint);
    signal(SIGPIPE, SIG_IGN);

#ifdef CLUSTER_ENABLED
    if (cluster_conf_path) {
        cluster_conf_t *cfg = cluster_conf_load(cluster_conf_path, port, cluster_bus_port ? cluster_bus_port : port + 10000);
        if (!cfg) {
            fprintf(stderr, "Errore nel caricamento cluster config\n");
            kv_close(g_db);
            return 1;
        }
        const char *my_id = "";
        if (cfg->my_node >= 0 && (size_t)cfg->my_node < cfg->node_count) {
            my_id = cfg->nodes[cfg->my_node].id;
        }
        load_or_create_nodeid_from_config(path ? path : "./.nodeid", port, my_id);
        g_cs = cluster_init(cfg, g_db);
        if (!g_cs) {
            fprintf(stderr, "Errore nell'inizializzazione cluster\n");
            cluster_conf_free(cfg);
            kv_close(g_db);
            return 1;
        }
        printf("Cluster enabled. My ID: %s\n", g_my_id);
    }
#endif

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    listen(srv, 64);
    printf("antzkv-server listening on port %d\n", port);

    while (g_running) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int client = accept(srv, (struct sockaddr*)&cli_addr, &cli_len);
        if (client < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        pthread_t tid;
        client_args_t *args = malloc(sizeof(client_args_t));
        args->fd = client;
        pthread_create(&tid, NULL, client_thread, args);
        pthread_detach(tid);
    }

    close(srv);
#ifdef CLUSTER_ENABLED
    if (g_cs) cluster_shutdown(g_cs);
#endif
    kv_close(g_db);
    return 0;
}
