# Optimization & Benchmarking Plan

---

## Phase 1 — 2PC over Kafka: Feasibility Analysis

**Goal**: Determine whether migrating 2PC from HTTP to Kafka improves performance, and whether it is worth the reduced code duplication.

### Reasoning

The client sends an HTTP Request to the system. This is first directed to the Gateway Service (NGINX). The Gateway Service strips the `/orders/` prefix from the URL and forwards the remaining part to the Order Service listening at Port 5000 (Docker Container) or `order-service:5000`.

Each Green Unicorn Worker is a separate OS process. When we say `-w 4`, we get 4 independent instances of OS processes – each with their own memory, python interpreter, and connection pool. They share nothing except the listening socket or the port. The `gunicorn` master process forks these worker processes and load balances the incoming connections across them. Hence the parallel execution.

So 1 Green Unicorn Worker = 1 OS Process = 1 Main Thread. Inside this single thread, `gevent` runs a scheduler loop called as "hub". Every incoming request to this main thread spawns its own `greenlet` or coroutine which is a simple pointer in the stack pointing to specific functions to run. When a greenlet hits an I/O operation, it yields the control back to the hub and the hub picks another available greenlet to run meanwhile. The greenlet runs voluntarily and takes as long as it needs to yield. This context switching is not preemptive (forcefully switched by the OS). If it does CPU-heavy work without yielding, it blocks the entire thread (main thread of the process). This is concurrency.

**HTTP 2PC** – Each PREPARE phase is sequential in nature. In the ideal happy case, there are 4 HTTP Round Trips in total. While one greenlet is waiting for a response, `gevent` can server other greenlets meanwhile.

Normally Kafka would spawn its own real OS thread. But we are using `gevent` and monkey patching. So the `threading` functions how spawn a greenlet instead of a real OS thread. The `start()` function starts a new greenlet thread managed by the hub. The `wait()` function does not block the main thread but makes the greenlet yield and wait. When `set()` is called from another greenlet, the waiting greenlet becomes runnable again. So 1 Green Unicorn Worker = 1 OS Process = 1 Main Thread. Inside this thread, multiple greenlets run cooperatively. Greenlet A – consuming from `gateway.orders`, Greenlet B – consuming from `internal.responses`, Greenlets C, D, E and so on – message handling greenlets spawned as the messages arrive. When Greenlet A is waiting for the messages from Gateway Service, it can wait and C or B can perform some operations. So everything is still single-threaded, cooperative and running on one single core.

**Kafka 2PC** – The "round trip" now involves 2 Kafka Broker hops instead of 1 HTTP Hop. The latency per hop is higher than the HTTP Hop. Proved in the table.

This is exactly what SAGA's `_fetch_item_price` already does — publish a request from Order Service to `internal.stocks`, block on the greenlet and switch to the one listening on `internal.responses`, wake up when the response arrives. It works, but every step adds latency.

- **HTTP round-trip**: Client opens connection → server processes → response returns. One network hop. With connection pooling, this is fast because we already have the connections open.
- **Kafka round-trip**: Producer serializes → broker appends to partition log → consumer polls (up to `fetch.max.wait.ms`) → consumer deserializes → processes → producer publishes response → broker appends → original consumer polls → deserializes. Two broker hops, two serialize/deserialize cycles, two poll intervals.

**Example with numbers**: Suppose each Kafka hop adds 5–10ms of latency (broker write + consumer poll + serialization). A 2PC checkout has 3 sequential round-trips (prepare stock, prepare payment, commit). That is 6 Kafka hops = 30–60ms of messaging overhead alone. HTTP with connection reuse does the same in 3 round-trips × 2–5ms = 6–15ms.

For SAGA, Kafka latency is acceptable because the flow is already asynchronous — the gateway returns to the client while the saga progresses in the background. For 2PC, the client is blocking the entire time, so every millisecond of messaging overhead directly increases user-visible latency.

**Cores In CPU** — A physical unit and can run one execution stream at a time. Can run multiple processes at the same time using a time-based scheduler. This is preemptive multitasking. But this is concurrency and not parallelism. True parallelism is 4 cores with 4 processes. The OS Scheduler decides which process should run on which core and can migrate things. When we say `-w 4`, we mean we use 4 cores. On a 8 core CPU, we already have 5 services each using 1 core. So a total of 5 cores are being used.

**Why we use** `-w 1` **in SAGA but can increase this in TPC?** – SAGA code uses in-memory dictionaries which will be different for each worker process. A response can be redirected by the Kafka master process to any of the processes which might not have the same pending process in their dictionary and will drop the message. The system breaks here.

**Python GIL** – Each worker instance has its own memory, python interpreter, and in-memory structures. Each interpreter allows only a single thread to execute the bytecode at a time. Hence the greenlets run one at a time. They wait till the GIL lock is released. Threads and greenlets are the same but greenlets are much lighter and have less overhead. For CPU bound tasks, none are helpful. We need multiple workers on multiple cores.

### What We Will Measure

We will add timing instrumentation to the existing codebase to capture:

- **End-to-end checkout latency** (measured at the order service, from request arrival to response)
- **Per-step latency** (prepare stock, prepare payment, commit — each measured individually)
- **Throughput** under concurrent load (checkouts per second)

We will run these measurements for the current HTTP-based 2PC and compare against the numbers we would get from Kafka-based 2PC branch we have.

### Decision


| Metric                   | HTTP 2PC | Kafka 2PC | Notes |
| ------------------------ | -------- | --------- | ----- |
| Median checkout latency  | —        | —         |       |
| P99 checkout latency     | —        | —         |       |
| Throughput (checkouts/s) | —        | —         |       |
| Code duplication reduced | No       | Yes       |       |


---

## Phase 2 — Baseline Benchmarking & Test Suite

**Goal**: Establish baseline performance numbers for the current system. Build a comprehensive, scriptable test suite that covers every scenario.

### 2A. Merge Group Member's Test Cases

Merge the additional test cases from the group member's branch into `baseline/optimizations`.

### 2B. Expand the Benchmarking Suite

The suite must cover:


| Category               | What It Tests                                                                       |
| ---------------------- | ----------------------------------------------------------------------------------- |
| **Correctness**        | CRUD operations, order lifecycle, non-existent resource handling                    |
| **Consistency**        | Multi-item checkout math, no oversell, no double-charge, no partial deductions      |
| **Concurrency**        | Last-item contention, parallel independent checkouts, concurrent addItem            |
| **Boundaries**         | Exact balance, exact stock, one-short-of-balance, one-short-of-stock, zero-quantity |
| **Idempotency**        | Duplicate addItem, duplicate checkout, duplicate stock add/subtract                 |
| **Failure & Recovery** | Participant crash mid-transaction, coordinator crash, stuck transaction recovery    |
| **Fault Injection**    | Kill a container mid-checkout, network partition simulation, database restart       |
| **Stress**             | Sustained load (Locust), throughput ceiling, latency under load                     |


### 2C. Operational Scripts

`**scripts/run.py`** — Container orchestration and parameter tuning.

```
python scripts/run.py <MODE> [options]

  MODE          TPC or SAGA
  -w, --workers Number of gunicorn workers per service (default: 1)
  -p, --partitions Kafka partition count (default: 3)
  -t, --timeout Request timeout in seconds (default: 30)
  --build       Force rebuild images
  --clean       Remove volumes before starting
```

`**scripts/bench.py**` — Execute the benchmarking suite.

```
python scripts/bench.py [options]

  --mode        TPC or SAGA (default: both)
  --suite       common, tpc, saga, stress, all (default: all)
  --output      Path for results JSON
  --repeat      Number of times to repeat each test (default: 1)
```

### Baseline Numbers


| Metric                        | TPC  | SAGA | Notes |
| ----------------------------- | ---- | ---- | ----- |
| Common suite pass rate        | —/16 | —/16 |       |
| Protocol suite pass rate      | —/12 | —/4  |       |
| Median checkout latency       | —    | —    |       |
| P99 checkout latency          | —    | —    |       |
| Peak throughput (checkouts/s) | —    | —    |       |
| Stress test error rate        | —    | —    |       |


---

## Phase 3 — Optimizations

**Goal**: Identify and fix performance bottlenecks. Every claim must be backed by measured numbers before and after.

### Methodology

1. Add timing instrumentation at every stage of the checkout flow.
2. Run the benchmarking suite, collect per-stage timings.
3. Identify the slowest stages (the bottleneck is the stage where the most time is spent).
4. Hypothesize a fix, implement it, re-run the suite, compare numbers.

### Bottleneck Log

*To be filled as we find and fix issues.*


| #   | Bottleneck | Evidence (before) | Fix Applied | Evidence (after) | Improvement |
| --- | ---------- | ----------------- | ----------- | ---------------- | ----------- |
| 1   | —          | —                 | —           | —                | —           |
| 2   | —          | —                 | —           | —                | —           |
| 3   | —          | —                 | —           | —                | —           |


---

## Phase 4 — Database Replication

**Goal**: Add PostgreSQL replication for high availability. Write tests that prove the system survives database failures.

### What We Implement

- Primary-replica PostgreSQL setup for each service database.
- Read queries routed to replicas, writes to primary.
- Automatic failover when primary dies.

### Test Cases to Add


| Test                                          | What It Proves                      |
| --------------------------------------------- | ----------------------------------- |
| Kill primary DB mid-checkout                  | System recovers without data loss   |
| Kill replica during read                      | Reads fall back to primary          |
| Kill primary, promote replica, resume traffic | Failover works end-to-end           |
| Verify replication lag under load             | Replicas stay within acceptable lag |


### Observations

*To be filled after implementation.*


| Metric                           | Without Replication | With Replication | Notes |
| -------------------------------- | ------------------- | ---------------- | ----- |
| Checkout latency (median)        | —                   | —                |       |
| Read latency (median)            | —                   | —                |       |
| Recovery time after primary kill | N/A                 | —                |       |
| Data loss after failover         | N/A                 | —                |       |


---

## Phase 5 — Sharding

**Goal**: Determine whether sharding is needed and where. Implement only if numbers justify it.

### Where Sharding Could Help


| Service | Candidate? | Reasoning                                                                         |
| ------- | ---------- | --------------------------------------------------------------------------------- |
| Stock   | Maybe      | If item count is very large, the items table becomes a hotspot. Shard by item_id. |
| Payment | Maybe      | If user count is very large, the users table becomes a hotspot. Shard by user_id. |
| Orders  | Unlikely   | Orders are keyed by UUID and rarely scanned. Lookups are by primary key.          |


### Decision Criteria

Sharding adds complexity (cross-shard queries, distributed joins, shard-aware routing). It is only worth it if:

1. A single database instance cannot sustain the required throughput (measured, not assumed).
2. Table size exceeds what fits in memory, causing disk I/O bottlenecks (measured via `pg_stat_user_tables`).
3. Lock contention on hot rows is the bottleneck (measured via `pg_stat_activity`).

### Observations

*To be filled after analysis.*


| Metric                  | Single DB | Sharded | Worth It? |
| ----------------------- | --------- | ------- | --------- |
| Throughput ceiling      | —         | —       | —         |
| Tail latency under load | —         | —       | —         |
| Lock contention rate    | —         | —       | —         |


---

## Phase 6 — Final Validation & Documentation

**Goal**: Run the complete suite one last time. Fix anything that regressed. Write the final README.

### Final Test Results

*To be filled after running.*


| Suite                              | Pass | Fail | Notes |
| ---------------------------------- | ---- | ---- | ----- |
| Common (correctness + consistency) | —    | —    |       |
| TPC protocol suite                 | —    | —    |       |
| SAGA protocol suite                | —    | —    |       |
| Fault injection                    | —    | —    |       |
| Stress test (Locust)               | —    | —    |       |


### Final Performance Numbers


| Metric                            | TPC | SAGA | Notes |
| --------------------------------- | --- | ---- | ----- |
| Median checkout latency           | —   | —    |       |
| P99 checkout latency              | —   | —    |       |
| Peak throughput                   | —   | —    |       |
| Recovery time (participant crash) | —   | —    |       |
| Recovery time (coordinator crash) | —   | —    |       |


### README Deliverable

The final `README.md` will contain:

1. How to start the system (one command).
2. How to switch between TPC and SAGA.
3. How to run the benchmarking suite.
4. How to tune parameters.
5. Description of every test case (one line each).
6. No reasoning, no justification — only instructions.

---

## Progress Tracker


| Phase                    | Status      | Branch/Commit |
| ------------------------ | ----------- | ------------- |
| 1. 2PC Kafka Analysis    | Not started | —             |
| 2. Baseline & Test Suite | Not started | —             |
| 3. Optimizations         | Not started | —             |
| 4. DB Replication        | Not started | —             |
| 5. Sharding              | Not started | —             |
| 6. Final Validation      | Not started | —             |


