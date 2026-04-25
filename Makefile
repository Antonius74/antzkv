CC = gcc
CFLAGS = -O2 -Wall -Wextra -Iinclude -pthread
LDFLAGS = -pthread

SRC_CORE = src/core/kvdb.c
SRC_SERVER = src/server/server.c
SRC_CLI = src/cli/cli.c

OBJ_CORE = build/core/kvdb.o
OBJ_SERVER = build/server/server.o
OBJ_CLI = build/cli/cli.o

.PHONY: all clean test

all: build/antzkv-server build/antzkv-cli

build/core/%.o: src/core/%.c | build/core
	$(CC) $(CFLAGS) -c $< -o $@

build/server/%.o: src/server/%.c | build/server
	$(CC) $(CFLAGS) -c $< -o $@

build/cli/%.o: src/cli/%.c | build/cli
	$(CC) $(CFLAGS) -c $< -o $@

build/antzkv-server: $(OBJ_CORE) $(OBJ_SERVER)
	$(CC) $(LDFLAGS) $^ -o $@

build/antzkv-cli: $(OBJ_CORE) $(OBJ_CLI)
	$(CC) $(LDFLAGS) $^ -o $@

build/core build/server build/cli:
	mkdir -p $@

clean:
	rm -rf build/

test: all
	@echo "=== Running functional test suite ==="
	@test/run_test.sh
