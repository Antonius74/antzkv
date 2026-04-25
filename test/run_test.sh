#!/bin/bash
set -eu

PORT=16379
PERSIST_PORT=16380
FILE=/tmp/antzkv_test_$$.dat
CLI="./build/antzkv-cli -p"
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

echo "=== Start server (memory only) ==="
./build/antzkv-server -p $PORT &
SERVER_PID=$!
sleep 1

# --- Ping ---
echo "=== PING ==="
R=$($CLI $PORT PING)
check "PING" "PONG" "$R"

# --- Basic SET/GET ---
echo "=== SET / GET basic ==="
R=$($CLI $PORT SET name Alice)
check "SET name" "OK" "$R"
R=$($CLI $PORT GET name)
check "GET name" "Alice" "$R"

R=$($CLI $PORT SET num 42)
check "SET num" "OK" "$R"
R=$($CLI $PORT GET num)
check "GET num" "42" "$R"

# --- Overwrite ---
echo "=== Overwrite ==="
R=$($CLI $PORT SET name Bob)
check "SET overwrite" "OK" "$R"
R=$($CLI $PORT GET name)
check "GET overwritten" "Bob" "$R"

# --- Missing key ---
echo "=== Missing key ==="
R=$($CLI $PORT GET missing)
check "GET missing" "(nil)" "$R"

# --- EXISTS ---
echo "=== EXISTS ==="
R=$($CLI $PORT EXISTS name)
check "EXISTS name" "1" "$R"
R=$($CLI $PORT EXISTS missing)
check "EXISTS missing" "0" "$R"

# --- KEYS ---
echo "=== KEYS ==="
R=$($CLI $PORT KEYS)
if echo "$R" | grep -q "name" && echo "$R" | grep -q "num"; then
    echo "[PASS] KEYS"
else
    echo "[FAIL] KEYS: expected 'name' and 'num', got '$R'"
    ERRORS=$((ERRORS+1))
fi

# --- DEL ---
echo "=== DEL ==="
R=$($CLI $PORT DEL name)
check "DEL name" "1" "$R"
R=$($CLI $PORT GET name)
check "GET after DEL" "(nil)" "$R"
R=$($CLI $PORT DEL missing)
check "DEL missing" "0" "$R"

# --- SAVE on memory-only (no file) ---
echo "=== SAVE in-memory ==="
R=$($CLI $PORT SAVE)
check "SAVE no-file" "ERR" "$R"

# --- Concurrent writes (5 parallel clients) ---
echo "=== Concurrent stress test ==="
for i in {1..20}; do
    $CLI $PORT SET "k$i" "v$i" >/dev/null &
done
wait
for i in {1..20}; do
    R=$($CLI $PORT GET "k$i")
    if [ "$R" != "v$i" ]; then
        echo "[FAIL] Concurrent GET k$i: expected v$i, got $R"
        ERRORS=$((ERRORS+1))
    fi
done
echo "[PASS] Concurrent stress (20 keys)"

# --- Stop server (graceful) ---
$CLI $PORT QUIT >/dev/null || true
sleep 1
kill $SERVER_PID 2>/dev/null || true

# ===== PERSISTENCE TEST =====
echo ""
echo "=== Persistence test ==="
./build/antzkv-server -p $PERSIST_PORT -f $FILE &
SERVER_PID=$!
sleep 1

R=$($CLI $PERSIST_PORT SET persist_key persist_val)
check "SET persistent" "OK" "$R"

R=$($CLI $PERSIST_PORT SAVE)
check "SAVE to file" "OK" "$R"

$CLI $PERSIST_PORT QUIT >/dev/null || true
sleep 1
kill $SERVER_PID 2>/dev/null || true

# Restart and reload
./build/antzkv-server -p $PERSIST_PORT -f $FILE &
SERVER_PID=$!
sleep 1

R=$($CLI $PERSIST_PORT GET persist_key)
check "GET after restart" "persist_val" "$R"

$CLI $PERSIST_PORT QUIT >/dev/null || true
sleep 1
kill $SERVER_PID 2>/dev/null || true

rm -f $FILE

echo ""
if [ $ERRORS -eq 0 ]; then
    echo "=== ALL TESTS PASSED ==="
    exit 0
else
    echo "=== $ERRORS TEST(S) FAILED ==="
    exit 1
fi
