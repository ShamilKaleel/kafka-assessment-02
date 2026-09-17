# Demo Cheat Sheet

One page to keep open while demonstrating. Commands, the four cases, and
what each one shows. (The full walkthrough is in `DEMO.md`.)

## Demo flow

1. **Setup** (done before the demo) — `make reset`, Kafka UI open, two terminals
2. **Theory** — Kafka, topics, partitions, offsets, Avro, retry, DLQ
3. **The system** — the overview diagram, the schema, the two topics in Kafka UI
4. **Case 1** — normal orders → running average
5. **Case 2** — temporary failure → retry → recovers
6. **Case 3** — permanent failure → Dead Letter Queue
7. **Case 4** — poison message → Dead Letter Queue, no retries
8. **Wrap-up** — consumer summary, tests, Git history

## Key terms

| Term | Meaning |
|---|---|
| Kafka | Messaging system: programs send messages to it, other programs read them |
| Producer | Program that sends messages (here: creates random orders) |
| Consumer | Program that reads and processes messages |
| Topic | Named channel messages go into (`orders`, `orders-dlq`) |
| Partition | A topic is split into partitions; this project uses 1 per topic |
| Offset | Position number of a message in a partition: 0, 1, 2, … |
| Consumer group | Named reader; Kafka remembers its offset so it resumes where it stopped (`order-consumer-group`) |
| Avro | Binary format for the message value; producer and consumer share one schema file (`schemas/order.avsc`) |
| Running average | Average price of all processed orders, recalculated after every message |
| Retry | On a temporary failure: wait, then try again — up to 3 attempts |
| Dead Letter Queue (DLQ) | Separate topic where permanently failed messages are parked with error headers |

## The system

Diagram: `docs/diagrams/07-high-level-overview.png`

- **Producer** → creates order `{orderId, product, price}` → Avro encode → topic `orders`
- **Consumer** ← reads `orders` → Avro decode → process → running average
  - processing fails → retry (max 3 attempts)
  - still failing / undecodable → topic `orders-dlq` (with headers: `error`, `original-offset`, `simulate-failure`)
  - then commit offset → next message
- **Kafka UI** (http://localhost:8080) → topics, partitions, offsets, messages (live), consumer lag
- Failures are simulated on request: the producer adds a `simulate-failure` header (`transient` / `permanent`); the order data itself never changes

## The four cases

Consumer runs once for all cases (Terminal 1: `make consumer`). Each case is one producer command in Terminal 2.

| # | Case | Command | Consumer shows | Running average | DLQ | Assignment requirement |
|---|---|---|---|---|---|---|
| 1 | Normal | `make producer ARGS="--count 5"` | green `RECEIVED` ×5 | updated ×5 | — | real-time aggregation |
| 2 | Temporary failure | `make producer ARGS="--count 3 --scenario transient --start-id 2001"` | yellow `RETRY` → bold-green `RECOVERED` → green `RECEIVED` | updated ×3 (still counted) | — | retry logic for temporary failures |
| 3 | Permanent failure | `make producer ARGS="--count 2 --scenario permanent --start-id 3001"` | `RETRY`, `RETRY`, red `FAILED`, red `DLQ` | unchanged | +2 | DLQ for permanently failed messages |
| 4 | Poison (not valid Avro) | `make producer ARGS="--poison"` | red `DLQ` immediately, no retries | unchanged | +1 | DLQ robustness — bad bytes can't be retried |

Optional: everything mixed in one stream — `make producer ARGS="--count 10 --scenario mixed --start-id 4001"` (6 normal, 2 transient, 2 permanent).

## What to point at, per case

| Case | Terminal | Kafka UI |
|---|---|---|
| 1 | `SENT` offsets 0…4; `RECEIVED` with `avg` and `n` growing | `orders` → Messages (live): 5 rows; value = binary (Avro), key = orderId |
| 2 | one `RETRY`, then `RECOVERED … attempt 2`, then `RECEIVED` | `orders` message → headers → `simulate-failure: transient`; `orders-dlq` still empty |
| 3 | three attempts, `giving up`, then `DLQ #3001 -> orders-dlq` | `orders-dlq` → Messages (live): 2 rows; headers `error`, `original-offset`, `simulate-failure: permanent` |
| 4 | `DLQ #poison` with no `RETRY` lines | `orders-dlq`: 3rd row, key `poison` |

After case 3 and 4: `make dlq` prints the DLQ as a table (decoded order, reason, original offset).

## Wrap-up

- **Terminal 1 → `Ctrl+C`** — summary table. Expected after cases 1–4:
  orders processed **8** · recovered after retry **3** · sent to DLQ **3** · final running average
- **`make test-unit`** — 13 tests: schema, Avro round-trip, running average, retry recovers, permanent gives up
- **GitHub → Commits** — one commit per feature

## Commands

| Command | Does |
|---|---|
| `make reset` | stop Kafka, wipe topic data, start fresh (topics re-created by `kafka-init`) |
| `make ps` | container status — kafka `healthy`, kafka-init `Exited (0)`, kafka-ui `Up` |
| `make consumer` | start the consumer (leave running) |
| `make producer ARGS="…"` | send orders — flags: `--count`, `--scenario normal\|transient\|permanent\|mixed`, `--poison`, `--start-id`, `--interval` |
| `make dlq` | print the Dead Letter Queue contents |
| `make test-unit` / `make test` | tests without / with Kafka |

## Colors in the terminal

| Color | Line | Meaning |
|---|---|---|
| green | `SENT` / `RECEIVED` | sent / processed, average updated |
| yellow | `RETRY` | attempt failed, trying again |
| bold green | `RECOVERED` | succeeded after a retry |
| bold red | `FAILED` / `DLQ` | last attempt failed / routed to the DLQ |
| cyan | banner / summary | configuration at start, totals at the end |
