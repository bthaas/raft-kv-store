# Distributed Key-Value Store

A fault-tolerant, horizontally scalable Key-Value Store built in Go using the Raft consensus algorithm.

## Features
-   **Strong Consistency**: Uses Raft (Leader Election, Log Replication) to ensure data is safe.
-   **Fault Tolerance**: Survives node failures in a 3-node cluster.
-   **Persistence**: Write-Ahead Log (WAL) for crash recovery.
-   **Simple API**: HTTP `POST /set` and `GET /get`.

## Architecture
-   **Leader Election**: Randomized election timers to elect a leader.
-   **Heartbeats**: Periodic empty AppendEntries RPCs to maintain authority.
-   **Log Replication**: Entries are replicated to a majority of followers before committing.
-   **Commit & Apply**: Once committed, entries are applied to the in-memory Store.

## Usage

### Prerequisites
-   Go 1.20+

### Running a 3-Node Cluster (Locally)

1.  **Build the Server**
    ```bash
    go build -o server cmd/server/main.go
    ```

2.  **Start 3 Terminals**

    **Terminal 1 (node1)**
    ```bash
    ./server -id node1 -port 8081 -peers localhost:8082,localhost:8083
    ```

    **Terminal 2 (node2)**
    ```bash
    ./server -id node2 -port 8082 -peers localhost:8081,localhost:8083
    ```

    **Terminal 3 (node3)**
    ```bash
    ./server -id node3 -port 8083 -peers localhost:8081,localhost:8082
    ```

### Interacting with the Cluster

**Write Data (Must talk to Leader)**
Try sending to any node. If it's not the leader, it will return 500 or 307 (currently 307/500 based on implementation check).

```bash
curl -v -X POST -d '{"key":"foo","value":"bar"}' http://localhost:8081/set
```

**Read Data**
```bash
curl "http://localhost:8081/get?key=foo"
```

### Fault Tolerance Test
1.  Identify the Leader (check logs for "Became Leader").
2.  Kill the Leader process (Ctrl+C).
3.  Watch the other nodes logs: one will start an election and become the new Leader.
4.  Write data to the new Leader.
5.  Restart the old Leader; it will rejoin as a Follower and catch up.
