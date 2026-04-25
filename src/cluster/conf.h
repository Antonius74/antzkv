#ifndef CLUSTER_CONF_H
#define CLUSTER_CONF_H

#include <stddef.h>

#define NODE_ID_LEN   16
#define NODE_HOST_LEN 64

/* replica mode */
typedef enum {
    REPL_AUTO = 0,
    REPL_DISK,
    REPL_MEMORY
} repl_mode_t;

typedef struct cluster_node_conf {
    char id[NODE_ID_LEN];
    char host[NODE_HOST_LEN];
    int client_port;
    int cluster_port;
    repl_mode_t repl;
} cluster_node_conf_t;

typedef struct cluster_conf {
    /* my_node è l'indice nella lista che rappresenta 'noi stessi' */
    cluster_node_conf_t *nodes;
    size_t node_count;
    int my_node;
    int my_cluster_port;
} cluster_conf_t;

/* Parse file path, returns NULL on error */
cluster_conf_t *cluster_conf_load(const char *path, int my_client_port, int my_cluster_port);
void cluster_conf_free(cluster_conf_t *cfg);

#endif
