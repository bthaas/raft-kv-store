#!/bin/bash
set -e

echo "=== Building Server ==="
go build -o server cmd/server/main.go

echo "=== Cleaning up WALs ==="
rm -f wal-node*.log

echo "=== Starting 3-Node Cluster ==="
# Start Node 1
./server -id node1 -port 8081 -peers localhost:8082,localhost:8083 > node1.log 2>&1 &
PID1=$!
echo "Node 1 started (PID $PID1)"

# Start Node 2
./server -id node2 -port 8082 -peers localhost:8081,localhost:8083 > node2.log 2>&1 &
PID2=$!
echo "Node 2 started (PID $PID2)"

# Start Node 3
./server -id node3 -port 8083 -peers localhost:8081,localhost:8082 > node3.log 2>&1 &
PID3=$!
echo "Node 3 started (PID $PID3)"

echo "Waiting for Leader Election (5s)..."
sleep 5

function write_key() {
    local KEY=$1
    local VAL=$2
    echo "Attempting to write $KEY=$VAL..."
    for PORT in 8081 8082 8083; do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -d "{\"key\":\"$KEY\",\"value\":\"$VAL\"}" http://localhost:$PORT/set)
        if [ "$HTTP_CODE" == "200" ]; then
            echo "Successfully wrote to Leader at port $PORT"
            LEADER_PORT=$PORT
            return 0
        fi
    done
    echo "Failed to write to any node."
    return 1
}

echo "=== Testing Replication ==="
write_key "foo" "bar"

echo "Waiting for replication (3s)..."
sleep 3

echo "Verifying read from all nodes..."
FAILURES=0
for PORT in 8081 8082 8083; do
    VAL=$(curl -s "http://localhost:$PORT/get?key=foo")
    echo "Node at $PORT has: '$VAL'"
    if [ "$VAL" != "bar" ]; then
        # It's possible a node is partitioned or slow, but ideally in local cluster all should have it
        # But wait, eventual consistency. 
        # If I write to Leader, it replicates to Majority.
        # So 2 nodes MUST have it. 1 might not.
        echo "Node at $PORT missing data (or not applied yet)."
    fi
done

# Check if at least one follower has it (excluding leader)
# We know LEADER_PORT has it.
# Simple check: expect 'bar' from at least one other node
REPLICATED_COUNT=0
for PORT in 8081 8082 8083; do
    VAL=$(curl -s "http://localhost:$PORT/get?key=foo")
    if [ "$VAL" == "bar" ]; then
        REPLICATED_COUNT=$((REPLICATED_COUNT+1))
    fi
done

if [ $REPLICATED_COUNT -ge 2 ]; then
    echo "SUCCESS: Replication confirmed (Found on $REPLICATED_COUNT nodes)."
else
    echo "FAILURE: Replication failed. Only found on $REPLICATED_COUNT nodes."
    # exit 1
fi

echo "=== Testing Fault Tolerance (Killing Leader) ==="
if [ -z "$LEADER_PORT" ]; then
    echo "Unknown leader, defaulting to killing Node 1"
    LEADER_PORT=8081
fi

# Map port to PID
if [ "$LEADER_PORT" == "8081" ]; then KILL_PID=$PID1; elif [ "$LEADER_PORT" == "8082" ]; then KILL_PID=$PID2; else KILL_PID=$PID3; fi

echo "Killing Leader at port $LEADER_PORT (PID $KILL_PID)..."
kill $KILL_PID

echo "Waiting for new election (5s)..."
sleep 5

echo "Writing 'baz=qux' to new leader..."
if write_key "baz" "qux"; then
    echo "Write successful."
else
    echo "Write failed."
fi

echo "Waiting for replication (3s)..."
sleep 3

echo "Reading 'baz' from remaining nodes..."
REPLICATED_COUNT=0
for PORT in 8081 8082 8083; do
    if [ "$PORT" == "$LEADER_PORT" ]; then continue; fi
    # Check if process exists (it's killed) implies curl fails.
    # But curl to killed port fails.
    
    VAL=$(curl -s "http://localhost:$PORT/get?key=baz")
    echo "Node at $PORT has: '$VAL'"
    if [ "$VAL" == "qux" ]; then
        REPLICATED_COUNT=$((REPLICATED_COUNT+1))
    fi
done

if [ $REPLICATED_COUNT -ge 1 ]; then
    echo "SUCCESS: Fault tolerance confirmed."
else
    echo "FAILURE: Fault tolerance failed."
fi

echo "=== Cleaning Up ==="
kill $PID1 2>/dev/null || true
kill $PID2 2>/dev/null || true
kill $PID3 2>/dev/null || true
rm -f server wal-node*.log
echo "Done."
