# MapReduce Word Count with RPC

A distributed MapReduce implementation for counting word frequencies in large text datasets using RPyC in distributed docker worker containers for inter-process communication.

## Description from HW2

1. The solution should implement a Coordinator and a Worker, each in ts own Docker container. Note that multiple workers should be used (3+). Communication between Coordinator and Workers must be implemented via RPyC (Remote Python Calls), and must occur via defined hostnames (e.g., “worker-1”, “coordinator”) resolved
through Docker networking.
2. Coordinator should allow the user to specify a URL to download a dataset. Assume it
can only download UTF-8 files.
3. Workers will periodically check or receive Tasks with/from the Coordinator via RPyC. A
Task can either be a Map or Reduce task.
4. Map tasks produce results that may be either returned to the Coordinator, or buffered
in memory in the Worker machine. The Map Worker is also responsible for partitioning
the intermediate pairs into its respective Regions. This is done using a partitioning
function. When a Worker completes a Map Task, it communicates with the Coordinator.
5. Reduce tasks are responsible for reducing the intermediate value keys found in all the
Worker containers. These are coordinated with Workers by the Coordinator.
6. The Coordinator aggregates the final outputs after all Reduce Workers return their
results. If an implementation allows concurrent updates to a shared state (e.g., a global
counter or store), those updates must be mutually exclusive (e.g., using locks,
semaphores, or transactional primitives) to ensure correctness under concurrent
access.
7. Map Tasks performed by a failed container need to be reassigned. If a Worker fails or
becomes unresponsive, its assigned Map (or Reduce) Task must be reassigned to
another available Worker. Failure can be assumed if a Worker exceeds a predefinedtimeout period for completing its assigned Task. The Coordinator is responsible for
detecting such failures and rescheduling tasks.
8. The Coordinator can exit when all Map and Reduce Tasks have finished.
9. Number of Map and Reduce Workers should be configurable via command-line
arguments or environment variables.

---

## Prerequisites

- Docker

---

## Getting Started

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd <repo-directory>
```

### 2. Build the images
```bash
docker compose build
```

### 3. Run with default settings (1 worker)
```bash
docker compose up
```

### 4. Clean up when done
```bash
docker compose down
```

---

## Scaling Workers

Run with different number of workers:

```bash
# 1 worker (default, slower, ~400s)
docker compose up

# 2 workers (better, ~80s)
WORKER_COUNT=2 docker compose up --scale docker-worker=2

# 4 workers (medium, ~120s)
WORKER_COUNT=4 docker compose up --scale docker-worker=4

# 8 workers (fantastic, ~60s)
WORKER_COUNT=8 CHUNK_SIZE=32 NO_OF_PARALLEL_THREADS=4 WAIT_TIME=20 docker compose up --scale worker=8
```

---


## Configuration

### Environment Variables (Docker)

Configure these in `docker-compose.yml` or pass them at runtime:

Adjust performance by setting environment variables when running:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_COUNT` | `1` | Number of worker processes |
| `WORKER_HOST` | `docker-worker` | Worker container hostname |
| `WORKER_PORT` | `18861` | RPC port for workers (docker-worker) |
| `WAIT_TIME` | `300` | RPC timeout in seconds (decrease for multiple workers) |
| `CHUNK_SIZE` | `8` | Chunks per worker (higher = better parallelism) |
| `NO_OF_PARALLEL_THREADS` | `1` | Parallel threads for RPC calls |


### Example execution

```bash
# Set environment variables inline
WORKER_COUNT=10 CHUNK_SIZE=32 NO_OF_PARALLEL_THREADS=4 WAIT_TIME=60 docker compose up --scale docker-worker=10
```

---

## How It Works

1. Coordinator downloads dataset (enwik9.zip from mattmahoney.net)
2. Splits text into chunks (WORKER_COUNT × CHUNK_SIZE chunks)
3. MAP: Workers tokenize text, filter stop words, count words
4. SHUFFLE: Coordinator groups word counts by key
5. REDUCE: Workers sum counts for assigned words
6. Coordinator displays top 20 most frequent words

If a worker fails or times out, the task is automatically retried on another worker.

---

## Performance Comparison

| Workers | Time | Notes |
|---------|------|-------|
| 1 | ~400s | Everything sequential |
| 3 | ~120s | Good balance (default) |
| 5 | ~80s | Diminishing returns start |
| 10 | ~60s | Near optimal for this dataset |
| Multicore | ~49s | Theoretical minimum (shared memory, no RPC) |

---

## Troubleshooting

**Clean everything and start fresh:**
```bash
docker compose down
docker system prune -a
docker compose build
docker compose up
```

---

## Testing Different Configurations

### Killing process (Fault-Tolerant Check)

Lets change the dataset and try :-
```bash
# Bring up with 32 workers
docker compose up --scale worker=32
```


```bash
# fetch all docker process and select any of the docker-worker process ID
docker ps

# get the process ID from the above list and kill one worker or two workers
docker kill <DOCKERPROCESSID>
```

Expected behaviour, coordinator should re-assign docker containers to other nodes.

### Changing or playing with parameters

```bash
# set the param you want from above config options and run
DATA_URL=https://mattmahoney.net/dc/enwik8.zip docker compose up
```

Expected behaviour, coordinator should get the enwik8 file and run


---
## Known Issues
1. For less than 8 workers the config should be NO_OF_PARALLEL_THREADS=1 WAIT_TIME=300 because of bottle neck.
2. Security issues with pickle library.

For any more issues please email mthiruma@ucsc.edu.

That's it. Build, run, scale. Happy MapReducing!