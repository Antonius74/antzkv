#!/bin/bash
set -eu

DIR="$(cd "$(dirname "$0")" && pwd)"
MODE="memory"  # default
NODES=3
BASE_CLIENT=6301
BASE_CLUSTER=17301
CONF=""
FORCE_REGEN=0

usage() {
    echo "Usage: $0 [--memory|--disk] [--force] [NODES] [CONFIG_FILE]"
    echo "  --memory   Run all nodes in-memory only (default)"
    echo "  --disk     Persist data to per-node .db files"
    echo "  --force    Regenerate config even if it exists"
    echo "  NODES      Number of nodes (default: 3)"
    echo "  CONFIG     Path to cluster.conf (auto-generated if missing)"
    exit 1
}

# Parse optional flags
while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --memory) MODE="memory"; shift ;;
        --disk)   MODE="disk";   shift ;;
        --force)  FORCE_REGEN=1;  shift ;;
        *) usage ;;
    esac
done

# Parse NODES and CONF if given
if [[ "${1:-}" =~ ^[0-9]+$ ]]; then
    NODES="$1"
    shift
fi

if [[ -n "${1:-}" ]]; then
    CONF="$1"
else
    CONF="$DIR/cluster.conf"
fi

should_regenerate() {
    local file="$1"
    if [ ! -f "$file" ]; then return 0; fi
    if [ "$FORCE_REGEN" -eq 1 ]; then return 0; fi
    # check if existing config has the requested number of nodes
    local existing=$(grep -c '^id=' "$file" 2>/dev/null || echo 0)
    if [ "$existing" -ne "$NODES" ]; then
        echo "Existing config has $existing nodes, requested $NODES. Regenerating..."
        return 0
    fi
    return 1
}

generate_config() {
    local file="$1"
    > "$file"
    echo "# Auto-generated cluster config ($MODE mode)" >> "$file"
    for i in $(seq 1 "$NODES"); do
        local id
        case $i in
            1) id="alpha";;
            2) id="beta";;
            3) id="gamma";;
            4) id="delta";;
            5) id="epsilon";;
            6) id="zeta";;
            7) id="eta";;
            8) id="theta";;
            *) id="node$i";;
        esac
        local cport=$((BASE_CLIENT + i - 1))
        local clport=$((BASE_CLUSTER + i - 1))
        echo "id=$id host=127.0.0.1 port=$cport:$clport replicate=$MODE" >> "$file"
    done
    echo "Generated $file with $NODES node(s) in $MODE mode"
}

if should_regenerate "$CONF"; then
    generate_config "$CONF"
fi

# Kill existing servers using this config
pkill -f "antzkv-server.*-c $CONF" >/dev/null 2>&1 || true
sleep 1

mkdir -p "$DIR/logs"
rm -f "$DIR/logs/cluster.pids"

echo "=== Starting antzkv cluster ($MODE mode) ==="
echo "Config: $CONF"
echo ""

# Collect node info
declare -a CLIENT_PORTS
declare -a CLUSTER_PORTS
declare -a NODE_IDS

while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// /}" ]] && continue

    port_field=$(echo "$line" | grep -o 'port=[^[:space:]]*' | cut -d= -f2)
    client_port=$(echo "$port_field" | cut -d: -f1)
    cluster_port=$(echo "$port_field" | cut -d: -f2)
    node_id=$(echo "$line" | grep -o 'id=[^[:space:]]*' | cut -d= -f2)

    CLIENT_PORTS+=("$client_port")
    CLUSTER_PORTS+=("$cluster_port")
    NODE_IDS+=("$node_id")
done < "$CONF"

# Start all nodes in parallel
for idx in "${!CLIENT_PORTS[@]}"; do
    cport="${CLIENT_PORTS[$idx]}"
    clport="${CLUSTER_PORTS[$idx]}"
    nid="${NODE_IDS[$idx]}"

    extra=""
    if [ "$MODE" = "disk" ]; then
        extra="-f $DIR/$nid.db"
    fi

    echo "  Starting node '$nid'  client=$cport  cluster=$clport"
    "$DIR/build/antzkv-server" -p "$cport" -c "$CONF" -C "$clport" $extra \
        > "$DIR/logs/${nid}.log" 2>&1 &
echo $! >> "$DIR/logs/cluster.pids"
done

# Wait for all nodes to be ready
echo ""
echo "Waiting for nodes to be ready..."
max_wait=30
for cport in "${CLIENT_PORTS[@]}"; do
    waited=0
    while [ $waited -lt $max_wait ]; do
        if "$DIR/build/antzkv-cli" -p "$cport" PING >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
        waited=$((waited + 1))
    done
done

echo ""
echo "All nodes started. Logs: $DIR/logs/*.log"
echo "PIDs:    $DIR/logs/cluster.pids"
echo ""

# Quick test on first node
first_port="${CLIENT_PORTS[0]}"
echo "Quick test:"
echo "  $DIR/build/antzkv-cli -p $first_port SET test_key test_value"
echo "  $DIR/build/antzkv-cli -p $first_port GET test_key"
# Actually run the test
sleep 1
$DIR/build/antzkv-cli -p $first_port SET test_key test_value >/dev/null 2>&1 || true
sleep 1
result=$($DIR/build/antzkv-cli -p $first_port GET test_key 2>/dev/null || true)
if [ "$result" = "test_value" ]; then
    echo ""
    echo "✅ Test PASSED on node 1"
else
    echo ""
    echo "⚠️  Test returned: '$result'"
fi

# Check replication across nodes - with retry loop
echo ""
echo "Testing cross-node replication (waiting for mesh to stabilize)..."
sleep 3

for retry in {1..30}; do
    replicated=0
    for cport in "${CLIENT_PORTS[@]}"; do
        result=$($DIR/build/antzkv-cli -p "$cport" GET test_key 2>/dev/null || true)
        if [ "$result" = "test_value" ]; then
            echo "  ✅ Port $cport: OK"
            replicated=$((replicated + 1))
        else
            echo "  ⚠️  Port $cport: waiting... (got: '$result')"
        fi
    done
    if [ "$replicated" -eq "${#CLIENT_PORTS[@]}" ]; then
        echo ""
        echo "🎉 ALL NODES REPLICATED SUCCESSFULLY!"
        break
    fi
    sleep 1
done

echo ""
echo "Stop all:  $DIR/stop-cluster.sh"
