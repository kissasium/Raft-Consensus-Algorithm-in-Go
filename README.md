# Raft Consensus Algorithm in Go

This project is an implementation of the Raft consensus algorithm in Go. It includes leader election, log replication, network partition handling, and quorum-based resolution upon network recovery.

## Features
- **Leader Election:** Automatically elects a leader among nodes.
- **Log Replication:** Ensures all nodes maintain a consistent state.
- **Network Partition Handling:** Supports multiple partitioned groups that hold separate elections.
- **Quorum Resolution:** On partition recovery, maintains logs and leader selection based on term numbers.
- **CLI Interaction:** Provides a command-line interface for interacting with the cluster.

## Installation & Setup
### Prerequisites
- Install [Go](https://go.dev/)

### Running the Raft Cluster
```sh
go run node.go
```

## CLI Commands
| Command | Description |
|---------|-------------|
| `put <key> <value>` | Store a key-value pair |
| `append <key> <value>` | Append a value to an existing key |
| `get <key>` | Retrieve a value by key |
| `store [id]` | Show KV store contents for a node (or all nodes) |
| `status` | Show status of all nodes |
| `leader` | Display the current leader |
| `partition <id>` | Partition a node from the network |
| `reset` | Reset all network partitions |
| `kill <id>` | Simulate a node crash and restart |
| `logs <id>` | Show logs of a specific node |
| `exit` | Exit the program |



