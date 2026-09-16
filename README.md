# Kafka Order Pipeline

A Kafka-based producer/consumer system for order messages: Avro
serialization, a real-time running average of prices, retry logic for
temporary failures, and a Dead Letter Queue (DLQ) for permanently failed
messages.

For the full write-up and diagrams, see [`docs/Assignment.md`](docs/Assignment.md)
(raw assignment text) and [`docs/Assignment-Explained.md`](docs/Assignment-Explained.md)
(step-by-step explanation with diagrams).

## Project structure

```
Makefile                   short commands for everything below
requirements.txt           Python dependencies (pinned)
pytest.ini                 test configuration
task.md                    task checklist and commit workflow
src/                       Python code (run as: python -m src.<module>)
  producer.py              order producer (Avro-encoded)
  consumer.py              order consumer (running average, retry, DLQ)
  dlq_reader.py            prints the contents of the DLQ with failure reasons
  config.py                shared settings (broker address, topic names, retry settings)
  avro_codec.py            Avro encode/decode using schemas/order.avsc
  processing.py            running average + retry logic (pure Python, unit-tested)
  dlq.py                   send to / read from the Dead Letter Queue
  console.py               colored terminal output
schemas/
  order.avsc               Avro schema for order messages
docker/
  docker-compose.yml       local Kafka broker (KRaft mode) + topic init + Kafka UI
  create-topics.sh         creates the orders and orders-dlq topics on startup
tests/
  test_codec.py            schema matches the assignment; Avro round-trip
  test_processing.py       running average, retry recovers, permanent gives up
  test_integration.py      end-to-end against the real broker (auto-skips if it's down)
docs/
  Assignment.md            raw extracted assignment text
  Assignment-Explained.md  step-by-step explanation with diagrams
  DEMO.md                  timed script for the 5-minute demo video
  diagrams/                architecture diagrams (.drawio sources + .png exports)
  Assignement Chapter 3.pdf  original assignment brief
```

## Prerequisites

- Docker + the Docker Compose plugin (`docker compose ...`)
- Python 3
- GNU make (optional — every `make` target below shows its raw command)

## Setup

1. Python environment:

   ```bash
   make venv        # = python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
   ```

2. Start Kafka (single broker, KRaft mode, no Zookeeper) and Kafka UI:

   ```bash
   make up          # = docker compose -f docker/docker-compose.yml up -d
   make ps          # kafka should show "healthy"
   ```

   This starts three containers: the Kafka broker, a one-shot `kafka-init`
   that creates the `orders` and `orders-dlq` topics and then exits (it shows
   as `Exited (0)` in `make ps` — that's success), and Kafka UI.

   The broker is reachable at `localhost:9092`. **Kafka UI** is a dashboard
   at http://localhost:8080 showing topics, partitions, offsets,
   consumer-group lag, and messages (it comes up ~30 s after the broker).
   Message *values* appear as raw bytes there — they're Avro-encoded and
   there's no schema registry to decode them — but keys, headers, and all
   the topic/consumer stats are readable.

## Running it

**Producer** — generates random orders, Avro-encodes them against
`schemas/order.avsc`, sends them to the `orders` topic:

```bash
make producer                                  # runs forever, ~1 order/sec
make producer ARGS="--count 5"                 # send exactly 5 then stop
make producer ARGS="--count 10 --scenario mixed"   # include simulated failures (see below)
make producer ARGS="--poison"                  # send one message that isn't valid Avro
# raw: .venv/bin/python -m src.producer --count 5
```

**Consumer** — reads from `orders`, Avro-decodes, prints each order with a
live running average of prices, retries temporary failures, and routes
permanent failures to the DLQ:

```bash
make consumer
make consumer ARGS="--max-attempts 3 --retry-delay 1"   # the defaults
# raw: .venv/bin/python -m src.consumer
```

### Failure scenarios (how the retry and DLQ paths are demonstrated)

Nothing in this pipeline fails on its own, so the **producer** can tag each
order with a `simulate-failure` header, and the consumer's processing step
honours it. The Avro payload itself is never changed — it always has exactly
the assignment's three fields.

| `--scenario` | Header on the message | What the consumer does | Assignment requirement |
|---|---|---|---|
| `normal` (default) | none | succeeds first time → running average updated | real-time aggregation |
| `transient` | `simulate-failure: transient` | attempt 1 fails ("downstream timeout"), attempt 2 succeeds → **recovered**, counted in the average | retry logic for temporary failures |
| `permanent` | `simulate-failure: permanent` | every attempt fails ("validation error") → after `--max-attempts` → **DLQ** | Dead Letter Queue for permanently failed messages |
| `mixed` | repeating cycle: normal, normal, transient, normal, permanent | all of the above in one stream (`--count 10` → 6 normal, 2 recovered, 2 DLQ) | everything at once |
| `--poison` | (message is not Avro at all) | can't be decoded → straight to the DLQ, no retries | DLQ robustness |

Consumer flags (optional, pass via `ARGS="..."`):

| Flag             | Default | Meaning                                                       |
|------------------|---------|----------------------------------------------------------------|
| `--max-attempts` | `3`     | Processing attempts before a message is treated as permanently failed. |
| `--retry-delay`  | `1.0`   | Seconds to wait between attempts.                              |

Messages routed to the `orders-dlq` topic keep their key (`orderId`), their
original Avro bytes and their original headers (so `simulate-failure` is
still visible), plus new headers `error`, `original-topic`,
`original-partition`, `original-offset` saying why and where they failed.

Stop either script with `Ctrl+C` — both shut down cleanly. The consumer
prints a summary table on exit (orders processed, recovered after retry,
sent to DLQ, final running average).

**Reading the terminal output** — every line is color-coded so the flow is
easy to follow on screen:

| Color        | Line         | Meaning                                              |
|--------------|--------------|------------------------------------------------------|
| green        | `SENT` / `RECEIVED` | order sent / order processed, running average updated |
| yellow       | `RETRY`      | an attempt failed, waiting and trying again          |
| bold green   | `RECOVERED`  | succeeded after one or more retries                  |
| bold red     | `FAILED` / `DLQ` | last attempt failed / message routed to `orders-dlq` |
| cyan         | banners, info | configuration at startup, summary on exit           |

Colors switch off automatically when output is redirected to a file.

**Inspect the DLQ** — prints a table of every message in `orders-dlq` with
the reason it failed and where it came from:

```bash
make dlq
# raw: .venv/bin/python -m src.dlq_reader
```

## Tests

```bash
make test-unit          # 13 quick checks, no Kafka needed (~0.1 s)
make test-integration   # 1 end-to-end run against the broker (~4 s; needs `make up`)
make test               # both
# raw: .venv/bin/python -m pytest -v
```

The unit tests cover the Avro schema/round-trip, the running average, and
the retry logic (transient failure recovers on the 2nd attempt, permanent
failure gives up after `MAX_ATTEMPTS`). The integration test produces a mix
of normal / transient / permanent / poison messages to its own temporary
topics, runs the consumer, and checks the stats and the DLQ contents —
then deletes the temporary topics.

## Live demo

For the recorded demo video (max 5 minutes), follow the timed script in
[`docs/DEMO.md`](docs/DEMO.md). The short version:

1. **Terminal 1**: `make consumer`
2. **Terminal 2**: `make producer ARGS="--count 10 --scenario mixed"` —
   watch Terminal 1: green `RECEIVED` lines with the running average, yellow
   `RETRY` then bold-green `RECOVERED` for the transient failures, and red
   `FAILED` → `DLQ` for the permanent ones.
3. Open Kafka UI (http://localhost:8080) → Topics → `orders-dlq` → Messages
   to show the failed messages with their `simulate-failure` and `error`
   headers, and/or `make dlq` to print them decoded in the terminal.
4. `Ctrl+C` the consumer to see the summary table.

**Tip:** the consumer's group (`order-consumer-group`) keeps its committed
offset across restarts, so re-running it won't replay old messages. The
simplest way to start a demo from a clean slate is `make reset` (stops
Kafka, wipes topic data, starts it again).

## Stopping

```bash
make down    # = docker compose -f docker/docker-compose.yml down      (keeps topic data)
make reset   # = ... down -v && ... up -d                              (wipes topic data, fresh start)
```
