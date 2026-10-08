# Distributed Task Scheduler in Rust

A robust, lightweight distributed task scheduler built from scratch in Rust using `tokio`, `axum`, and `reqwest`.

## Architecture

- **Master Node**: Coordinates task queuing, task distribution, worker registration, and heartbeat monitoring.
- **Worker Node**: Registers with the master, periodically sends heartbeats, polls for pending tasks, executes them asynchronously, and reports results.
- **Client CLI**: Allows users to submit new tasks and inspect task statuses.

## Getting Started

### 1. Build the project
```bash
cargo build --release
```

### 2. Run the Master Node
```bash
cargo run --bin master -- --port 8080
```

### 3. Run Worker Node(s)
Open separate terminal windows for workers:
```bash
cargo run --bin worker -- --worker-id worker-1 --master http://127.0.0.1:8080
```
```bash
cargo run --bin worker -- --worker-id worker-2 --master http://127.0.0.1:8080
```

### 4. Submit Tasks using the Client CLI
```bash
# Submit a task
cargo run --bin client -- submit "Compute Fibonacci of 40"

# List all tasks and their statuses
cargo run --bin client -- list
```
