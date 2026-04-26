#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
PIDFILE="$DIR/logs/cluster.pids"

if [ -f "$PIDFILE" ]; then
    echo "=== Stopping cluster ==="
    while read -r pid; do
        if kill "$pid" >/dev/null 2>&1; then
            echo "  Killed PID $pid"
        fi
    done < "$PIDFILE"
    rm -f "$PIDFILE"
    echo "All nodes stopped."
else
    echo "No PID file found at $PIDFILE"
    echo "Trying pkill fallback..."
    pkill -f "antzkv-server.*-c $DIR/cluster.conf" >/dev/null 2>&1 || true
fi
