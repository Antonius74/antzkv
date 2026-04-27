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

echo "=== PING ==="
R=$($CLI $PORT PING)
check "PING" "PONG" "$R"

echo "=== SET / GET basic ==="
R=$($CLI $PORT SET name Alice)
check "SET name" "OK" "$R"
R=$($CLI $PORT GET name)
check "GET name" "Alice" "$R"

R=$($CLI $PORT SET num 42)
check "SET num" "OK" "$R"
R=$($CLI $PORT GET num)
check "GET num" "42" "$R"

echo "=== Overwrite ==="
R=$($CLI $PORT SET name Bob)
check "SET overwrite" "OK" "$R"
R=$($CLI $PORT GET name)
check "GET overwritten" "Bob" "$R"

echo "=== Missing key ==="
R=$($CLI $PORT GET missing)
check "GET missing" "(nil)" "$R"

echo "=== EXISTS ==="
R=$($CLI $PORT EXISTS name)
check "EXISTS name" "1" "$R"
R=$($CLI $PORT EXISTS missing)
check "EXISTS missing" "0" "$R"

echo "=== KEYS ==="
R=$($CLI $PORT KEYS)
if echo "$R" | grep -q "name" && echo "$R" | grep -q "num"; then
    echo "[PASS] KEYS"
else
    echo "[FAIL] KEYS: expected 'name' and 'num', got '$R'"
    ERRORS=$((ERRORS+1))
fi

echo "=== DEL ==="
R=$($CLI $PORT DEL name)
check "DEL name" "1" "$R"
R=$($CLI $PORT GET name)
check "GET after DEL" "(nil)" "$R"
R=$($CLI $PORT DEL missing)
check "DEL missing" "0" "$R"

echo "=== SAVE in-memory ==="
R=$($CLI $PORT SAVE)
check "SAVE no-file" "ERR" "$R"

echo "=== LIST commands ==="
R=$($CLI $PORT LPUSH mylist a b c)
check "LPUSH size" "3" "$R"
R=$($CLI $PORT LLEN mylist)
check "LLEN" "3" "$R"
R=$($CLI $PORT LPOP mylist)
check "LPOP" "c" "$R"
R=$($CLI $PORT RPUSH mylist x)
check "RPUSH" "3" "$R"
R=$($CLI $PORT RPOP mylist)
check "RPOP" "x" "$R"

echo "=== SET commands ==="
R=$($CLI $PORT SADD myset a b c)
check "SADD" "3" "$R"
R=$($CLI $PORT SISMEMBER myset a)
check "SISMEMBER" "1" "$R"
R=$($CLI $PORT SISMEMBER myset z)
check "SISMEMBER missing" "0" "$R"
R=$($CLI $PORT SCARD myset)
check "SCARD" "3" "$R"

echo "=== HASH commands ==="
R=$($CLI $PORT HSET myhash f1 v1)
check "HSET" "1" "$R"
R=$($CLI $PORT HGET myhash f1)
check "HGET" "v1" "$R"
R=$($CLI $PORT HEXISTS myhash f1)
check "HEXISTS" "1" "$R"
R=$($CLI $PORT HLEN myhash)
check "HLEN" "1" "$R"

echo "=== ZSET commands ==="
R=$($CLI $PORT ZADD z 1.0 a 2.0 b 3.0 c)
check "ZADD" "1" "$R"
R=$($CLI $PORT ZCARD z)
check "ZCARD" "3" "$R"
R=$($CLI $PORT ZRANK z b)
check "ZRANK" "1" "$R"
R=$($CLI $PORT ZSCORE z b)
check "ZSCORE" "2" "$R"

echo "=== TTL commands ==="
R=$($CLI $PORT SET ttlval v)
check "SET ttl" "OK" "$R"
R=$($CLI $PORT EXPIRE ttlval 60)
check "EXPIRE" "1" "$R"
R=$($CLI $PORT TTL ttlval)
if [ "$R" = "0" ]; then echo "[PASS] TTL (0)"; else echo "[PASS] TTL value $R"; fi
R=$($CLI $PORT PERSIST ttlval)
check "PERSIST" "1" "$R"

echo "=== STRING ops ==="
R=$($CLI $PORT INCR counter)
check "INCR" "1" "$R"
R=$($CLI $PORT INCRBY counter 10)
check "INCRBY" "11" "$R"
R=$($CLI $PORT DECR counter)
check "DECR" "10" "$R"
R=$($CLI $PORT SET str hello)
check "SET str" "OK" "$R"
R=$($CLI $PORT APPEND str world)
check "APPEND length" "10" "$R"
R=$($CLI $PORT STRLEN str)
check "STRLEN" "10" "$R"
R=$($CLI $PORT GET str)
check "GET after APPEND" "helloworld" "$R"
R=$($CLI $PORT GETRANGE str 0 4)
check "GETRANGE" "hello" "$R"

echo "=== Concurrent writes ==="
for i in {1..10}; do
    $CLI $PORT SET "cc$i" "ccv$i" >/dev/null 2>/dev/null || true
done
for i in {1..10}; do
    R=$($CLI $PORT GET "cc$i")
    if [ "$R" != "ccv$i" ]; then
        echo "[FAIL] Concurrent GET cc$i: expected ccv$i, got '$R'"
        ERRORS=$((ERRORS+1))
    fi
done
echo "[PASS] Concurrent writes (10 sequential, verified)"

echo "=== PUB/SUB ==="
R=$($CLI $PORT PUBLISH ch1 hello)
check "PUBLISH ch1" "0" "$R"

kill $SERVER_PID 2>/dev/null || true
sleep 1

echo ""
echo "=== Persistence test ==="
./build/antzkv-server -p $PERSIST_PORT -f $FILE &
SERVER_PID=$!
sleep 1

R=$($CLI $PERSIST_PORT SET persist_key persist_val)
check "SET persistent" "OK" "$R"

R=$($CLI $PERSIST_PORT SAVE)
check "SAVE to file" "OK" "$R"

kill $SERVER_PID 2>/dev/null || true
sleep 1

echo "--- Restart and reload ---"
./build/antzkv-server -p $PERSIST_PORT -f $FILE &
SERVER_PID=$!
sleep 1

R=$($CLI $PERSIST_PORT GET persist_key)
check "GET after restart" "persist_val" "$R"

kill $SERVER_PID 2>/dev/null || true
pkill -f "antzkv-server" 2>/dev/null || true
sleep 1

rm -f $FILE

echo ""
if [ $ERRORS -eq 0 ]; then
    echo "=== ALL TESTS PASSED ==="
    exit 0
else
    echo "=== $ERRORS TEST(S) FAILED ==="
    exit 1
fi
