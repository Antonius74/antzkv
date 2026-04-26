#!/bin/bash
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT1=17379
PORT2=17380
CLUSTER1=27379
CLUSTER2=27380
CLI="$DIR/build/antzkv-cli -p"
SVR="$DIR/build/antzkv-server"
CONF="$DIR/test/cluster_test.conf"
ERRORS=0

check() {
    local label="$1"
    local expected="$2"
    local got="$3"
    if [ "$expected" != "$got" ]; then
        echo "[FAIL] $label: expected '$expected', got '$got'"
        ERRORS=$((ERRORS+1))
    else
        echo "[PASS] $label"
    fi
}

# --- pulizia ---
pkill -f "antzkv-server -p $PORT1" && sleep 1 || true
pkill -f "antzkv-server -p $PORT2" && sleep 1 || true
rm -f "$CONF"

# --- scrivi configurazione cluster ---
cat > "$CONF" <<EOF
# Cluster test config
id=alpha host=127.0.0.1 port=$PORT1:$CLUSTER1 replicate=memory
id=beta  host=127.0.0.1 port=$PORT2:$CLUSTER2 replicate=memory
EOF

echo "=== Avvio nodo alpha ($PORT1 / $CLUSTER1) ==="
$SVR -p $PORT1 -c "$CONF" -C $CLUSTER1 > "$DIR/logs/test_alpha.log" 2>&1 &
PID1=$!
sleep 1

echo "=== Avvio nodo beta ($PORT2 / $CLUSTER2) ==="
$SVR -p $PORT2 -c "$CONF" -C $CLUSTER2 > "$DIR/logs/test_beta.log" 2>&1 &
PID2=$!

# --- Attesa stabilizzazione mesh + retry ---
echo "=== Attesa connessione cluster... ==="
for i in {1..60}; do
    if $CLI $PORT1 PING >/dev/null 2>&1 && $CLI $PORT2 PING >/dev/null 2>&1; then
        echo "=== Nodi pronti (tentativo $i) ==="
        break
    fi
    sleep 0.5
done
sleep 1

# --- Scrittura su alpha ---
echo "=== Scrittura cluster ==="
R=$($CLI $PORT1 SET shared_key shared_val)
check "SET su alpha" "OK" "$R"

# --- Attesa replica con retry ---
wait_for_key() {
    local port=$1
    local key=$2
    local expected=$3
    local label=$4
    for i in {1..30}; do
        R=$($CLI $port GET $key)
        if [ "$R" = "$expected" ]; then
            check "$label" "$expected" "$R"
            return 0
        fi
        sleep 0.5
    done
    check "$label" "$expected" "$R"
    return 1
}

wait_for_key $PORT2 shared_key shared_val "GET su beta (replica)"

# --- Overwrite su beta, lettura su alpha ---
R=$($CLI $PORT2 SET shared_key overwritten)
check "SET su beta" "OK" "$R"
wait_for_key $PORT1 shared_key overwritten "GET su alpha (replica overwrite)"

# --- DEL su alpha ---
R=$($CLI $PORT1 DEL shared_key)
check "DEL su alpha" "1" "$R"
wait_for_key $PORT2 shared_key "(nil)" "GET su beta dopo DEL"

# --- Stop ---
$CLI $PORT1 QUIT >/dev/null || true
$CLI $PORT2 QUIT >/dev/null || true
sleep 1
kill $PID1 2>/dev/null || true
kill $PID2 2>/dev/null || true

rm -f "$CONF"

echo ""
if [ $ERRORS -eq 0 ]; then
    echo "=== ALL CLUSTER TESTS PASSED ==="
    exit 0
else
    echo "=== $ERRORS CLUSTER TEST(S) FAILED ==="
    exit 1
fi
