#!/bin/bash
set -eu

DIR="$(cd "$(dirname "$0")" && pwd)"
MODE="memory"  # default
NODES=3
BASE_CLIENT=6301
BASE_CLUSTER=17301
CONF=""

usage() {
    echo "Usage: $0 [--memory|--disk] [NODES] [CONFIG_FILE]"
    echo "  --memory   Run all nodes in-memory only (default)"
    echo "  --disk     Persist data to per-node .db files"
    echo "  NODES      Number of nodes (default: 3)"
    echo "  CONFIG     Path to cluster.conf (auto-generated if missing)"
    exit 1
}

# Parse optional flags
while [[ "$1" == --* ]]; do
    case "$1" in
        --memory) MODE="memory"; shift ;;
        --disk)   MODE="disk";   shift ;;
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

generate_config() {
    local file="$1"
    echo "# Auto-generated cluster config ($MODE mode)" > "$file"
    for i in $(seq 1 "$NODES"); do
        local letter=$(printf '%x' $((i + 96)) | xxd -r -p 2>/dev/null || printf \\$(printf '%o' $((i + 96))))
        local id
        case $i in
            1) id="alpha";;
            2) id="beta";;
            3) id="gamma";;
            4) id="delta";;
            5) id="epsilon";;
            *) id="node$i";;
        esac
        local cport=$((BASE_CLIENT + i - 1))
        local clport=$((BASE_CLUSTER + i - 1))
        echo "id=$id host=127.0.0.1 port=$cport:$clport replicate=$MODE" >> "$file"
    done
    echo "Generated $file with $NODES node(s) in $MODE mode"
}

# Generate config if missing
if [ ! -f "$CONF" ]; then
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

while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// /}" ]] && continue

    port_field=$(echo "$line" | grep -o 'port=[^[:space:]]*' | cut -d= -f2)
    client_port=$(echo "$port_field" | cut -d: -f1)
    cluster_port=$(echo "$port_field" | cut -d: -f2)
    node_id=$(echo "$line" | grep -o 'id=[^[:space:]]*' | cut -d= -f2)

    extra=""
    if [ "$MODE" = "disk" ]; then
        extra="-f $DIR/$node_id.db"
    fi

    echo "  Node '$node_id'  client=$client_port  cluster=$cluster_port  $extra"
    "$DIR/build/antzkv-server" -p "$client_port" -c "$CONF" -C "$cluster_port" $extra \
        > "$DIR/logs/$node_id.log" 2>&1 &
    echo $! >> "$DIR/logs/cluster.pids"
done < "$CONF"

echo ""
echo "All nodes started. Logs: $DIR/logs/*.log"
echo "PIDs:    $DIR/logs/cluster.pids"
echo ""

# Quick hint
client_ports=$(grep -o 'port=[^[:space:]]*' "$CONF" | cut -d: -f1 | cut -d= -f2 | head -1)
echo "Quick test:"
echo "  $DIR/build/antzkv-cli -p $client_ports SET test_key test_value"
echo "  $DIR/build/antzkv-cli -p $client_ports GET test_key"
echo ""
echo "Stop all:  $DIR/stop-cluster.sh"
