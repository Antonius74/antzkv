#include "conf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

static int parse_field_str(const char *line, const char *key, char *out, size_t out_len) {
    size_t kl = strlen(key);
    const char *p = line;
    while (*p) {
        p = strstr(p, key);
        if (!p) return 0;
        if ((p == line || isspace((unsigned char)p[-1])) && p[kl] == '=') {
            const char *val = p + kl + 1;
            size_t vl = 0;
            while (val[vl] && !isspace((unsigned char)val[vl])) ++vl;
            if (vl >= out_len) vl = out_len - 1;
            memcpy(out, val, vl);
            out[vl] = '\0';
            return 1;
        }
        p += kl;
    }
    return 0;
}

cluster_conf_t *cluster_conf_load(const char *path, int my_client_port, int my_cluster_port) {
    FILE *fp = fopen(path, "r");
    if (!fp) return NULL;

    cluster_conf_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) { fclose(fp); return NULL; }

    char line[256];
    while (fgets(line, sizeof(line), fp)) {
        char *p = line;
        while (isspace((unsigned char)*p)) ++p;
        if (*p == '#' || *p == '\0' || *p == '\n') continue;

        cluster_node_conf_t node = {0};
        node.client_port = my_client_port;
        node.cluster_port = my_cluster_port;
        node.repl = REPL_AUTO;

        if (!parse_field_str(line, "id", node.id, sizeof(node.id))) continue;
        if (!parse_field_str(line, "host", node.host, sizeof(node.host))) continue;

        char port_str[32];
        if (parse_field_str(line, "port", port_str, sizeof(port_str))) {
            char *colon = strchr(port_str, ':');
            if (colon) {
                *colon = '\0';
                node.client_port = atoi(port_str);
                node.cluster_port = atoi(colon + 1);
            } else {
                node.client_port = atoi(port_str);
            }
        }

        char repl_str[16];
        if (parse_field_str(line, "replicate", repl_str, sizeof(repl_str))) {
            if (strcmp(repl_str, "disk") == 0) node.repl = REPL_DISK;
            else if (strcmp(repl_str, "memory") == 0) node.repl = REPL_MEMORY;
        }

        cluster_node_conf_t *tmp = realloc(cfg->nodes, (cfg->node_count + 1) * sizeof(cluster_node_conf_t));
        if (!tmp) { cluster_conf_free(cfg); fclose(fp); return NULL; }
        cfg->nodes = tmp;
        cfg->nodes[cfg->node_count] = node;
        cfg->node_count++;
    }
    fclose(fp);

    if (cfg->node_count == 0) {
        free(cfg);
        return NULL;
    }

    cfg->my_node = -1;
    for (size_t i = 0; i < cfg->node_count; ++i) {
        if (cfg->nodes[i].client_port == my_client_port) {
            cfg->my_node = (int)i;
            break;
        }
    }
    if (cfg->my_node < 0) cfg->my_node = 0;
    cfg->my_cluster_port = my_cluster_port;
    return cfg;
}

void cluster_conf_free(cluster_conf_t *cfg) {
    if (!cfg) return;
    free(cfg->nodes);
    free(cfg);
}
