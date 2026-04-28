#include "kvdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <editline/readline.h>
#include <errno.h>
#include <termios.h>

#define BUFFER_SIZE 4096

static struct termios orig_termios;
static int tty_raw = 0;

static void tty_reset(void) {
    if (tty_raw) {
        tcsetattr(STDIN_FILENO, TCSANOW, &orig_termios);
        tty_raw = 0;
    }
}

static void tty_set_raw(void) {
    if (tty_raw) return;
    struct termios raw;
    tcgetattr(STDIN_FILENO, &orig_termios);
    raw = orig_termios;
    raw.c_lflag &= ~(ICANON | ECHO);
    raw.c_cc[VMIN]  = 1;
    raw.c_cc[VTIME] = 0;
    tcsetattr(STDIN_FILENO, TCSANOW, &raw);
    tty_raw = 1;
    atexit(tty_reset);
}

static int connect_server(const char *host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(sock); return -1;
    }
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock); return -1;
    }
    return sock;
}

static int send_cmd(int sock, const char *cmd) {
    size_t len = strlen(cmd);
    size_t total = len + 1;
    char *buf = malloc(total);
    memcpy(buf, cmd, len);
    buf[len] = '\n';
    ssize_t n = write(sock, buf, total);
    free(buf);
    return (n == (ssize_t)total) ? 0 : -1;
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

static int read_line_raw(int sock, char *buf, size_t len) {
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

static int recv_resp_raw(int sock, char *out, size_t out_len) {
    char prefix;
    ssize_t n = read(sock, &prefix, 1);
    if (n <= 0) { out[0] = '\0'; return -1; }

    if (prefix == '+')               return read_line_raw(sock, out, out_len);
    if (prefix == ':')               return read_line_raw(sock, out, out_len);

    if (prefix == '-') {
        read_line_raw(sock, out, out_len);
        return -1;
    }

    if (prefix == '$') {
        char lenbuf[32];
        if (read_line_raw(sock, lenbuf, sizeof(lenbuf)) < 0) {
            out[0] = '\0'; return -1;
        }
        int blen = atoi(lenbuf);
        if (blen < 0) {
            snprintf(out, out_len, "(nil)");
            return -1;
        }
        if (blen == 0) {
            out[0] = '\0';
            char crlf[2]; read_exact(sock, crlf, 2);
            return 0;
        }
        if ((size_t)blen >= out_len - 1) { out[0]='\0'; return -1; }
        if (read_exact(sock, out, (size_t)blen) < 0) { out[0]='\0'; return -1; }
        out[blen] = '\0';
        char crlf[2]; read_exact(sock, crlf, 2);
        return blen;
    }

    if (prefix == '*') {
        char lenbuf[32];
        if (read_line_raw(sock, lenbuf, sizeof(lenbuf)) < 0) {
            out[0] = '\0'; return -1;
        }
        int arrlen = atoi(lenbuf);
        if (arrlen <= 0) {
            snprintf(out, out_len, "(empty)");
            return -1;
        }
        /* Read elements, then format */
        char elements[128][BUFFER_SIZE];
        int el_count = 0;
        for (int i = 0; i < arrlen && el_count < 128; ++i) {
            char elem[BUFFER_SIZE];
            int r = recv_resp_raw(sock, elem, sizeof(elem));
            if (r < 0) snprintf(elem, sizeof(elem), "(nil)");
            if (el_count < 128) {
                strncpy(elements[el_count], elem, BUFFER_SIZE - 1);
                elements[el_count][BUFFER_SIZE - 1] = '\0';
                ++el_count;
            }
        }
        /* Pub/Sub message detection */
        if (arrlen == 3 && el_count == 3 &&
            strcmp(elements[0], "message") == 0) {
            snprintf(out, out_len, "%s: %s", elements[1], elements[2]);
            return (int)strlen(out);
        }
        /* Subscribe/unsubscribe ACK */
        if (arrlen == 3 && el_count >= 1 &&
            (strcmp(elements[0], "subscribe") == 0 ||
             strcmp(elements[0], "unsubscribe") == 0)) {
            snprintf(out, out_len, "%s %s %s",
                     elements[0], elements[1], elements[2]);
            return (int)strlen(out);
        }
        /* Generic array */
        out[0] = '\0';
        size_t pos = 0;
        for (int i = 0; i < el_count; ++i) {
            if (i > 0 && pos < out_len - 2) {
                out[pos++] = ' '; out[pos] = '\0';
            }
            size_t slen = strlen(elements[i]);
            if (pos + slen >= out_len) break;
            memcpy(out + pos, elements[i], slen);
            pos += slen; out[pos] = '\0';
        }
        return (int)pos;
    }
    out[0] = '\0';
    return -1;
}

/* ==================================================================
   Pub/Sub raw mode — select() on stdin + socket for instant delivery
   ================================================================== */
static void pubsub_read_loop(int sock) {
    tty_set_raw();
    printf("\r\033[K-- Subscribed. Messages appear in real-time. --\n\n");
    fd_set rfds;
    char   linebuf[BUFFER_SIZE * 2];
    size_t linepos = 0;
    int    sub_active = 1;

    while (sub_active) {
        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);
        FD_SET(STDIN_FILENO, &rfds);
        int maxfd = (sock > STDIN_FILENO) ? sock : STDIN_FILENO;
        struct timeval tv = {0, 50000};
        int ready = select(maxfd + 1, &rfds, NULL, NULL, &tv);
        if (ready < 0) { if (errno == EINTR) continue; break; }

        /* Server data */
        if (FD_ISSET(sock, &rfds)) {
            char resp[BUFFER_SIZE];
            int r = recv_resp_raw(sock, resp, sizeof(resp));
            if (r < 0 && resp[0] == '\0') break;
            if (resp[0] != '\0') {
                printf("\r\033[K%s\n", resp);
                if (linepos > 0) printf("> %s", linebuf);
                fflush(stdout);
            }
        }

        /* User input (raw, char by char) */
        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            char c;
            if (read(STDIN_FILENO, &c, 1) <= 0) break;
            if (c == '\n' || c == '\r') {
                printf("\r\033[K\n"); fflush(stdout);
                if (linepos > 0) {
                    linebuf[linepos] = '\0';
                    if (strcasecmp(linebuf, "QUIT") == 0) {
                        send_cmd(sock, "QUIT");
                        sub_active = 0;
                        break;
                    }
                    send_cmd(sock, linebuf);
                }
                linepos = 0; linebuf[0] = '\0';
            } else if (c == 127 || c == '\b') {
                if (linepos > 0) {
                    --linepos; linebuf[linepos] = '\0';
                    printf("\b \b"); fflush(stdout);
                }
            } else if (c >= ' ') {
                if (linepos < sizeof(linebuf) - 2) {
                    linebuf[linepos++] = c;
                    linebuf[linepos] = '\0';
                    putchar(c); fflush(stdout);
                }
            }
        }
    }
    tty_reset();
}

/* ==================================================================
   Normal REPL (readline-based)
   ================================================================== */
static void repl_loop(int sock) {
    printf("Connected. Type commands (QUIT to exit).\n");

    while (1) {
        char *line = readline("kvdb> ");
        if (!line) break;
        if (strlen(line) == 0) { free(line); continue; }
        add_history(line);

        int is_sub = (strncasecmp(line, "SUBSCRIBE", 9) == 0);

        if (send_cmd(sock, line) < 0) { free(line); break; }

        /* Read subscribe ACKs before entering Pub/Sub mode */
        if (is_sub) {
            int channels = 1;
            char *p = line + 9;
            while (*p) { if (*p == ' ') ++channels; ++p; }
            for (int i = 0; i < channels; ++i) {
                char resp[BUFFER_SIZE];
                int r = recv_resp_raw(sock, resp, sizeof(resp));
                if (r < 0) break;
                if (resp[0] != '\0') printf("%s\n", resp);
            }
            free(line);
            pubsub_read_loop(sock);
            printf("[back to kvdb>]\n");
            continue;
        }

        /* Normal reply */
        char resp[BUFFER_SIZE];
        int r = recv_resp_raw(sock, resp, sizeof(resp));
        if (r >= 0 && resp[0] != '\0') printf("%s\n", resp);
        else if (r < 0 && resp[0] != '\0') printf("%s\n", resp);

        int is_quit = (strncasecmp(line, "QUIT", 4) == 0);
        free(line);
        if (is_quit) break;
    }
}

/* ================================================================== */
/*  Main                                                               */
/* ================================================================== */
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
        int r = recv_resp_raw(sock, resp, sizeof(resp));
        if (r >= 0 && resp[0] != '\0') puts(resp);
        else if (r < 0 && resp[0] != '\0') puts(resp);
        close(sock);
        return 0;
    }

    int sock = connect_server(host, port);
    if (sock < 0) { perror("connect"); return 1; }
    repl_loop(sock);
    close(sock);
    return 0;
}
