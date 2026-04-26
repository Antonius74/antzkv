#include "kvdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <editline/readline.h>

#define BUFFER_SIZE 4096

static int connect_server(const char *host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

static int send_cmd(int sock, const char *cmd) {
    if (write(sock, cmd, strlen(cmd)) < 0) return -1;
    if (write(sock, "\n", 1) < 0) return -1;
    return 0;
}

static int recv_line(int sock, char *buf, size_t len) {
    size_t i = 0;
    char c;
    while (i < len - 1) {
        ssize_t n = read(sock, &c, 1);
        if (n <= 0) return -1;
        if (c == '\n') break;
        if (c != '\r') buf[i++] = c;
    }
    buf[i] = '\0';
    return (int)i;
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 6379;

    int i = 1;
    while (i < argc) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) {
            host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            port = atoi(argv[++i]);
        } else {
            break;
        }
        ++i;
    }

    if (i < argc) {
        /* Modalità non-interattiva */
        int sock = connect_server(host, port);
        if (sock < 0) { perror("connect"); return 1; }
        char cmd[BUFFER_SIZE];
        cmd[0] = '\0';
        for (int j = i; j < argc; ++j) {
            if (j > i) strncat(cmd, " ", sizeof(cmd) - strlen(cmd) - 1);
            strncat(cmd, argv[j], sizeof(cmd) - strlen(cmd) - 1);
        }
        send_cmd(sock, cmd);
        char resp[BUFFER_SIZE];
        if (recv_line(sock, resp, sizeof(resp)) >= 0) {
            if (resp[0] != '\0') {
                puts(resp);
            }
        }
        return 0;
    }

    /* Modalità interattiva con readline e history */
    int sock = connect_server(host, port);
    if (sock < 0) { perror("connect"); return 1; }
    printf("Connesso a %s:%d.\nDigita i comandi (QUIT per uscire).\n", host, port);

    char resp[BUFFER_SIZE];
    while (1) {
        char *line = readline("kvdb> ");
        if (!line) break; /* EOF (Ctrl+D) */

        if (strlen(line) == 0) {
            free(line);
            continue;
        }

        add_history(line);

        if (send_cmd(sock, line) < 0) {
            free(line);
            break;
        }
        if (recv_line(sock, resp, sizeof(resp)) >= 0)
            printf("%s\n", resp);

        int is_quit = (strncasecmp(line, "QUIT", 4) == 0);
        free(line);
        if (is_quit) break;
    }
    close(sock);
    return 0;
}
