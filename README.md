# Kafka Order Pipeline

A Kafka-based producer/consumer system for order messages: Avro
serialization, a real-time running average of prices, retry logic for
temporary failures, and a Dead Letter Queue (DLQ) for permanently failed
messages.

For the full write-up and diagrams, see `Assignment.md` (raw assignment
text) and `Assignment-Explained.md` (step-by-step explanation with
diagrams).

## Prerequisites

- Docker + the Docker Compose plugin (`docker compose ...`)
- Python 3

## Setup

1. Start Kafka (single broker, KRaft mode, no Zookeeper):

   ```bash
   docker compose up -d
   docker compose ps   # should show "healthy"
   ```

   The broker is reachable at `localhost:9092`.

2. Set up the Python environment:

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

## Running it

**Producer** — generates random orders, Avro-encodes them against
`order.avsc`, sends them to the `orders` topic:

```bash
.venv/bin/python3 producer.py                       # runs forever, ~1 order/sec
.venv/bin/python3 producer.py --count 5              # send exactly 5 then stop
.venv/bin/python3 producer.py --count 5 --interval 0.2  # faster, for quick tests
```

**Consumer** — reads from `orders`, Avro-decodes, prints each order with a
live running average of prices:

```bash
.venv/bin/python3 consumer.py
```

Retry/DLQ flags (all optional):

| Flag             | Default | Meaning                                                       |
|------------------|---------|----------------------------------------------------------------|
| `--fail-rate`    | `0.0`   | Probability [0-1] a processing attempt simulates a failure. Demo-only knob — nothing in this pipeline fails on its own, so this is how you make the retry/DLQ path actually trigger. |
| `--max-retries`  | `3`     | Attempts before a message is treated as permanently failed.   |
| `--retry-delay`  | `1.0`   | Seconds to wait between retries.                               |

Permanently failed messages are routed to the `orders-dlq` topic, keyed by
`orderId`, carrying the original Avro bytes plus headers (`error`,
`original-topic`, `original-partition`, `original-offset`).

Stop either script with `Ctrl+C` — both shut down cleanly.

## Live demo script

1. **Terminal 1** — start the consumer normally:
   ```bash
   .venv/bin/python3 consumer.py
   ```
2. **Terminal 2** — start the producer:
   ```bash
   .venv/bin/python3 producer.py
   ```
   Watch orders flow through and the running average update live in
   Terminal 1.
3. Stop the consumer (`Ctrl+C`), then restart it with simulated failures on
   to show the retry → DLQ path:
   ```bash
   .venv/bin/python3 consumer.py --fail-rate 1.0 --max-retries 3 --retry-delay 1
   ```
   With the producer still running (or send a couple more with
   `--count 3`), you'll see `processing failed: ...` lines repeat up to
   `--max-retries` times, then `routed to DLQ orderId=... -> orders-dlq
   [...]` — that confirmation line is the DLQ evidence, no separate
   inspection tool needed.

**Tip:** the consumer's group (`order-consumer-group`) keeps its committed
offset across restarts, so re-running it won't replay old messages. To
replay everything from the start for a clean demo (with no consumer
running):

```bash
docker exec kafka-assessment2 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group order-consumer-group \
  --reset-offsets --to-earliest --topic orders --execute
```

## Stopping

```bash
docker compose down       # stop the broker, keep topic data
docker compose down -v    # also wipe topic data for a totally fresh start
```

## Project structure

```
Assignment.md              raw extracted assignment text
Assignment-Explained.md    step-by-step explanation with diagrams
task.md                    task checklist and commit workflow
diagrams/                  architecture diagrams (.drawio sources + .png exports)
docker-compose.yml         local Kafka broker (KRaft mode)
order.avsc                 Avro schema for order messages
producer.py                order producer (Avro-encoded)
consumer.py                order consumer (running average, retry, DLQ)
requirements.txt           Python dependencies
```
