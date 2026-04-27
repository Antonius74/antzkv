CC = gcc
CFLAGS = -O2 -Wall -Wextra -Iinclude -Isrc -pthread -DCLUSTER_ENABLED
LDFLAGS = -pthread
ifeq ($(shell uname),Linux)
  LDFLAGS_CLI = -pthread -lreadline
else
  LDFLAGS_CLI := -pthread -ledit
endif

SRC_CORE = src/core/kvdb.c
SRC_SERVER = src/server/server.c
SRC_CLI = src/cli/cli.c
SRC_CLUSTER = src/cluster/conf.c src/cluster/cluster.c

OBJ_CORE = build/core/kvdb.o
OBJ_SERVER = build/server/server.o
OBJ_CLI = build/cli/cli.o
OBJ_CLUSTER = build/cluster/conf.o build/cluster/cluster.o

.PHONY: all clean test

all: build/antzkv-server build/antzkv-cli

build/core/%.o: src/core/%.c | build/core
	$(CC) $(CFLAGS) -c $< -o $@

build/server/%.o: src/server/%.c | build/server
	$(CC) $(CFLAGS) -c $< -o $@

build/cli/%.o: src/cli/%.c | build/cli
	$(CC) $(CFLAGS) -c $< -o $@

build/cluster/%.o: src/cluster/%.c | build/cluster
	$(CC) $(CFLAGS) -c $< -o $@

build/antzkv-server: $(OBJ_CORE) $(OBJ_SERVER) $(OBJ_CLUSTER)
	$(CC) $(LDFLAGS) $^ -o $@

build/antzkv-cli: $(OBJ_CORE) $(OBJ_CLI)
	$(CC) $^ $(LDFLAGS_CLI) -o $@

build/core build/server build/cli build/cluster:
	mkdir -p $@

clean:
	rm -rf build/

test: all
	@echo "=== Running functional test suite ==="
	@test/run_test.sh
