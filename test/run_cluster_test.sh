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
$SVR -p $PORT1 -c "$CONF" -C $CLUSTER1 &
PID1=$!
sleep 1

echo "=== Avvio nodo beta ($PORT2 / $CLUSTER2) ==="
$SVR -p $PORT2 -c "$CONF" -C $CLUSTER2 &
PID2=$!
sleep 2

# --- Scrittura su alpha ---
echo "=== Scrittura cluster ==="
R=$($CLI $PORT1 SET shared_key shared_val)
check "SET su alpha" "OK" "$R"

# --- Attesa replica ---
sleep 1

# --- Lettura su beta ---
R=$($CLI $PORT2 GET shared_key)
check "GET su beta (replica)" "shared_val" "$R"

# --- Overwrite su beta, lettura su alpha ---
R=$($CLI $PORT2 SET shared_key overwritten)
check "SET su beta" "OK" "$R"
sleep 1
R=$($CLI $PORT1 GET shared_key)
check "GET su alpha (replica overwrite)" "overwritten" "$R"

# --- DEL su alpha ---
R=$($CLI $PORT1 DEL shared_key)
check "DEL su alpha" "1" "$R"
sleep 1
R=$($CLI $PORT2 GET shared_key)
check "GET su beta dopo DEL" "(nil)" "$R"

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
