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

static int read_exact(int sock, char *buf, size_t n) {
    size_t r = 0;
    while (r < n) {
        ssize_t got = read(sock, buf + r, n - r);
        if (got <= 0) return -1;
        r += (size_t)got;
    }
    return 0;
}

static int read_line(int sock, char *buf, size_t len) {
    size_t i = 0;
    while (i < len - 1) {
        char c;
        ssize_t n = read(sock, &c, 1);
        if (n <= 0) return -1;
        if (c == '\n') break;
        if (c != '\r') buf[i++] = c;
    }
    buf[i] = '\0';
    return (int)i;
}

static int recv_resp(int sock, char *out, size_t out_len) {
    char prefix;
    ssize_t n = read(sock, &prefix, 1);
    if (n <= 0) { out[0] = '\0'; return -1; }

    if (prefix == '+') {
        return read_line(sock, out, out_len);
    }
    if (prefix == '-') {
        read_line(sock, out, out_len);
        return -1;
    }
    if (prefix == ':') {
        return read_line(sock, out, out_len);
    }
    if (prefix == '$') {
        char lenbuf[32];
        if (read_line(sock, lenbuf, sizeof(lenbuf)) < 0) {
            out[0] = '\0'; return -1;
        }
        int blen = atoi(lenbuf);
        if (blen < 0) {
            snprintf(out, out_len, "(nil)");
            return -1;
        }
        if ((size_t)blen >= out_len - 1) {
            out[0] = '\0'; return -1;
        }
        if (read_exact(sock, out, (size_t)blen) < 0) {
            out[0] = '\0'; return -1;
        }
        out[blen] = '\0';
        char crlf[2];
        read_exact(sock, crlf, 2);
        return blen;
    }
    if (prefix == '*') {
        char lenbuf[32];
        if (read_line(sock, lenbuf, sizeof(lenbuf)) < 0) {
            out[0] = '\0'; return -1;
        }
        int arrlen = atoi(lenbuf);
        if (arrlen < 0) {
            snprintf(out, out_len, "(empty)");
            return -1;
        }
        out[0] = '\0';
        size_t pos = 0;
        for (int i = 0; i < arrlen; ++i) {
            char elem[BUFFER_SIZE];
            int r = recv_resp(sock, elem, sizeof(elem));
            if (r < 0) {
                if (pos < out_len - 1) {
                    snprintf(out + pos, out_len - pos, "(nil)");
                    pos += 5;
                }
                continue;
            }
            if (i > 0 && pos < out_len - 2) { out[pos++] = ' '; out[pos] = '\0'; }
            size_t elen = strlen(elem);
            if (pos + elen >= out_len) break;
            memcpy(out + pos, elem, elen);
            pos += elen;
            out[pos] = '\0';
        }
        return (int)pos;
    }
    out[0] = '\0';
    return -1;
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 6379;

    int i = 1;
    while (i < argc) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc)
            host = argv[++i];
        else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc)
            port = atoi(argv[++i]);
        else break;
        ++i;
    }

    if (i < argc) {
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
        if (recv_resp(sock, resp, sizeof(resp)) >= 0) {
            if (resp[0] != '\0') {
                puts(resp);
            }
        } else if (resp[0] != '\0') {
            puts(resp);
        }
        close(sock);
        return 0;
    }

    int sock = connect_server(host, port);
    if (sock < 0) { perror("connect"); return 1; }
    printf("Connesso a %s:%d.\nDigita i comandi (QUIT per uscire).\n",
           host, port);

    while (1) {
        char *line = readline("kvdb> ");
        if (!line) break;
        if (strlen(line) == 0) { free(line); continue; }
        add_history(line);
        if (send_cmd(sock, line) < 0) { free(line); break; }

        char resp[BUFFER_SIZE];
        int r = recv_resp(sock, resp, sizeof(resp));
        if (resp[0] != '\0' || r < 0) {
            if (r < 0 && resp[0] != '\0') printf("%s\n", resp);
            else if (r >= 0 && resp[0] != '\0') printf("%s\n", resp);
        }

        int is_quit = (strncasecmp(line, "QUIT", 4) == 0);
        free(line);
        if (is_quit) break;
    }
    close(sock);
    return 0;
}
