# Distributed Data Systems — Group 20

Aditya Patil · Danil Vorotilov · Pedro Gomes Moreira · Ruben Van Seventer · Veselin Mitev

---

## How To Run

### Requirements

- Docker + Docker Compose
- Python 3.11+ (test runner only)

### Start

```bash
# 2PC mode (default)
docker compose up -d --build

# SAGA mode
TRANSACTION_MODE=SAGA NGINX_CONF=gateway_nginx_saga.conf docker compose up -d --build
```

### Stop

```bash
docker compose down -v
```

---

## Configuration

Pass as environment variables before `docker compose up`, or write to a `.env` file at the project root.


| Variable                | Default              | Description                                                                                            |
| ----------------------- | -------------------- | ------------------------------------------------------------------------------------------------------ |
| `TRANSACTION_MODE`      | `TPC`                | Protocol used by all services. `TPC` for two-phase commit, `SAGA` for event-driven compensation.       |
| `NGINX_CONF`            | `gateway_nginx.conf` | Nginx routing config. Must be `gateway_nginx_saga.conf` when `TRANSACTION_MODE=SAGA`.                  |
| `REDIS_MAX_CONNECTIONS` | `6000`               | Connection pool size per Redis pool per service worker. Size for expected peak concurrency.            |
| `STREAM_BATCH_SIZE`     | `500`                | Messages fetched per `XREADGROUP` call. All messages in a batch are processed concurrently via gevent. |


Example `.env` for a smaller local machine:

```env
REDIS_MAX_CONNECTIONS=1200
STREAM_BATCH_SIZE=100
```

---

## Test Suite

The test suite is in `test/`. It is a standalone CLI — no external test framework. It builds the stack, runs all cases, and reports results.

### Run

```bash
cd test
pip install -r ../requirements.txt

python run.py                                    # TPC mode, full build and fresh stack
python run.py --mode SAGA                        # SAGA mode
python run.py --skip-build                       # skip docker compose build
python run.py --no-restart                       # reuse an already-running stack
BASE_URL=http://192.168.1.10:8000 python run.py  # custom gateway address
```

### Suite Order

The runner executes suites in this order:

1. **Common** — mode-agnostic correctness and durability
2. **2PC or SAGA** — protocol-specific correctness (selected by `--mode`)
3. **Replication** — Sentinel failover and data durability (runs last because it changes cluster topology)

---

## Test Cases

### Common Suite


| Test                               | What It Verifies                                                                                                               |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Multi-Item Checkout                | A checkout containing multiple distinct items deducts the correct quantity from each item's stock and charges the exact total. |
| Double Checkout                    | A second checkout on an already-paid order is a no-op — no duplicate stock deduction or charge.                                |
| Post-Checkout Tampering            | Adding items to a paid order and retrying checkout produces no extra charge and no stock deduction.                            |
| Empty Order Checkout               | Checking out an order with no items returns 200 without crashing any service.                                                  |
| 10 Concurrent Checkouts For 1 Unit | Ten users race to buy the last unit; exactly one succeeds and stock never goes negative.                                       |
| Sequential Drain                   | Sequential checkouts exhaust stock in order; the request that would exceed available stock is rejected.                        |
| 5 Independent Parallel Checkouts   | Five isolated concurrent checkouts each succeed without interfering with each other.                                           |
| External Stock Change              | Reducing stock between order creation and checkout causes the checkout to fail correctly.                                      |
| Fund User After Order              | Adding sufficient credit after order creation but before checkout allows the checkout to succeed.                              |
| Balance Exactly Equals Total       | Checkout succeeds when the user's credit equals the order cost exactly.                                                        |
| Stock Exactly Equals Quantity      | Checkout succeeds when available stock equals the ordered quantity exactly.                                                    |
| One Credit Short                   | Checkout is rejected when the user is exactly one credit unit below the required amount.                                       |
| One Stock Unit Short               | Checkout is rejected when stock is exactly one unit below the ordered quantity.                                                |
| Non-Existent Resource Lookup       | GET requests for random or non-existent IDs return 4xx, not 5xx.                                                               |
| addItem Idempotency                | Sending the same add-item request twice with the same idempotency key applies the change only once.                            |
| Partial Stock Failure              | If any item in a multi-item batch is out of stock, the entire checkout fails atomically — no partial deductions.               |
| Batch Init                         | `batch_init` creates integer-keyed items and users that are immediately readable via the API.                                  |
| Add-Stock Idempotency              | Duplicate add-stock requests with the same idempotency key do not double the stock.                                            |
| Pay Idempotency                    | Duplicate add-funds requests with the same idempotency key do not double the credit.                                           |
| Order Item Merge                   | Adding the same item to an order twice merges the quantities into a single line item.                                          |
| Zero/Negative Amount Rejected      | `add_stock` and `add_funds` reject amounts ≤ 0 with a 4xx response.                                                            |
| Item Not Found Subtract            | Subtracting stock from a non-existent item returns 4xx, not 5xx.                                                               |
| User Not Found Pay                 | Charging a non-existent user returns 4xx, not 5xx.                                                                             |
| Malformed Stream Message           | A garbage message injected directly into the stream does not crash consumers; the next valid request succeeds.                 |
| Payment Redis AOF Durability       | Credit written to Redis survives a container restart; checkout succeeds after the restart.                                     |
| Stock Redis AOF Durability         | Stock written to Redis survives a container restart; checkout succeeds after the restart.                                      |
| Concurrent Oversell Prevention     | 20 users racing to buy 3 units result in exactly 3 successful checkouts and exactly 3 charges — no oversell.                   |
| Stock Service Crash Mid-Batch      | Killing and restarting the stock service during a concurrent batch produces no oversell and no phantom charges.                |
| Redis Bus Restart Recovery         | Restarting the shared message bus does not prevent subsequent checkouts from completing.                                       |


---

### 2PC Suite


| Test                       | What It Verifies                                                                                                        |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| PREPARE → COMMIT           | PREPARE holds the reservation and reduces visible stock/credit; COMMIT makes the deduction permanent.                   |
| PREPARE → ABORT            | PREPARE holds the reservation; ABORT returns all reserved resources to their original values.                           |
| Vote NO                    | PREPARE for more than available is rejected with 4xx; no reservation is made and nothing changes.                       |
| PREPARE Locks Resources    | A PREPARE-held reservation is unavailable to concurrent regular subtract operations.                                    |
| ABORT Releases Resources   | After ABORT, the previously locked resources are available for new operations.                                          |
| Competing PREPAREs         | Two concurrent PREPAREs on the same insufficient stock — the first wins, the second votes NO.                           |
| Idempotent COMMIT          | Calling COMMIT twice on the same transaction produces no double deduction.                                              |
| ABORT Safety               | ABORT on a non-existent or already-committed transaction returns 200 and changes nothing.                               |
| Stock Votes NO             | When stock cannot fulfil, the coordinator aborts the entire checkout — payment is untouched.                            |
| Payment Votes NO           | When payment cannot fulfil, the coordinator aborts and fully reverses the stock reservation.                            |
| Participant Crash Recovery | Stock service killed mid-checkout restarts and the coordinator successfully retries the operation.                      |
| Coordinator Crash Recovery | A stuck in-doubt transaction injected directly into the log is deterministically resolved by `recovery_tpc` on restart. |


---

### SAGA Suite


| Test                             | What It Verifies                                                                                                                        |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| Compensating Transaction         | Stock is reserved, then payment fails; the saga fires a compensating transaction that fully restores stock.                             |
| Stock Fails, No Payment          | Immediate stock failure causes early rejection — the payment step is never attempted.                                                   |
| Participant Crash Recovery       | Stock service killed mid-saga; the stream's pending re-delivery mechanism completes the saga after restart.                             |
| Coordinator Crash Recovery       | A stuck `STOCK_REQUESTED` saga injected into the log is consistently resolved (committed or compensated) by `recovery_saga` on restart. |
| Double-Checkout Prevention       | A second checkout on an already-paid order is detected and skipped; stock and credit are unchanged.                                     |
| Multi-Item Compensation          | All items from a failed multi-item checkout are fully restored by the compensating transaction — no partial rollback.                   |
| Concurrent Correlation Isolation | 20 simultaneous checkouts each receive the correct, isolated response via their correlation ID.                                         |
| Pending-Message Re-Delivery      | A stream message left unACKed due to a service crash is re-delivered and processed correctly after restart.                             |


---

### Replication Suite


| Test                                | What It Verifies                                                                                                                      |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Payment Primary SIGKILL             | Data written before a hard kill of the payment primary survives on the promoted replica; checkouts continue through the new master.   |
| Coordinator DB SIGKILL Mid-Checkout | Killing the order-service's Redis mid-checkout and restarting order-service resolves the transaction consistently with no data loss.  |
| Bus Primary SIGKILL With In-Flight  | Killing the message bus during 10 concurrent checkouts results in no phantom charges or oversell after Sentinel promotes the replica. |
| One Sentinel Down                   | Killing one of three sentinels still leaves quorum intact; a subsequent primary failure is correctly failed over.                     |
| 50 Writes Before Primary SIGKILL    | All 50 items written to a primary before a hard kill are readable from the promoted replica — zero data loss on promotion.            |


