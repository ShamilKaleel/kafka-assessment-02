# Demo Video Script (max 5 minutes)

A timed, step-by-step script for recording the demonstration video. Follow
it top to bottom. Each segment has the exact command to run and a short
line to say. The whole thing runs about 4 min 20 s, leaving buffer.

The demo uses **one** consumer run. The producer sends a mix of normal
orders, orders that fail once and recover (retry), and orders that fail
permanently (Dead Letter Queue) — so every requirement shows up in the same
stream, color-coded.

## Before you press record

Do all of this first — none of it needs to be on camera. Run every command
from the project folder.

1. Fresh Kafka, so the recording starts from a clean state:
   ```bash
   make reset            # stop, wipe topic data, start fresh
   make ps               # wait until kafka says "healthy" (~15 s)
   ```
2. Open **Kafka UI** at http://localhost:8080 in a browser and wait until the
   `local` cluster shows as online (it starts ~30 s after Kafka is healthy).
   Leave it on the **Topics** page.
3. Open **two terminals side by side**, both in the project folder, with a
   large font (the viewer has to read the log lines).
4. Have these ready to show:
   - `docs/diagrams/03-system-architecture.png` (open in an image viewer)
   - `schemas/order.avsc` (open in the editor)
   - the GitHub repo's **Commits** page in another browser tab
5. Nothing running in either terminal yet.

## The recording

### 0:00 — Intro (25 s)

Show: `docs/diagrams/03-system-architecture.png`

Say: *"This is my Kafka assignment. A producer sends order messages into
Kafka, and a consumer reads them. Every message is Avro-encoded. The
consumer keeps a running average of prices, retries temporary failures, and
sends permanently failed messages to a dead letter queue."*

### 0:25 — The Avro schema (15 s)

Show: `schemas/order.avsc`

Say: *"Each order has three fields — orderId, product, and price. This
schema file is shared by the producer and the consumer, so both sides agree
on the message format."*

### 0:40 — Everything in one run (90 s)

**Terminal 1** (consumer):
```bash
make consumer
```
**Terminal 2** (producer), a moment later:
```bash
make producer ARGS="--count 10 --scenario mixed"
```

The producer sends 10 orders: 6 normal, 2 tagged *transient*, 2 tagged
*permanent* (the tag shows at the end of each `SENT` line). Narrate what
appears in Terminal 1, colour by colour:

- green `RECEIVED` lines — *"Each order is decoded from Avro and the running
  average price is recalculated after every message — live, one at a time."*
- yellow `RETRY` then bold-green `RECOVERED` — *"This order failed with a
  temporary error. The consumer waited and tried again, and the second
  attempt succeeded — so it's still counted in the average. That's the retry
  logic for temporary failures."*
- yellow `RETRY`, `RETRY`, red `FAILED` then red `DLQ` — *"This one fails
  every time. After three attempts it's treated as permanently failed and
  routed to the `orders-dlq` topic — and the consumer moves on instead of
  crashing."*

(Nothing in the pipeline fails on its own, so the producer adds a
`simulate-failure` header to request these failures. The order data itself
is never changed.)

(On a fresh cluster the consumer prints one `UNKNOWN_TOPIC_OR_PART` line
before the first order — that's expected; it picks the topic up by itself.)

### 2:10 — Kafka UI (35 s)

Switch to the browser tab with **Kafka UI**, on the **Topics** page. Click
**orders-dlq** → **Messages** tab → expand one message.

Say: *"The Kafka dashboard shows both topics — `orders` with all ten orders,
and `orders-dlq` with the two that failed permanently. Opening a failed
message shows its headers: the error reason, where it came from, and the
failure tag that was requested."*

(The message *value* shows as raw bytes here — that's the Avro encoding,
which the dashboard can't decode without a schema registry. The decoded
orders are shown in the terminal instead.)

### 2:45 — The DLQ from code (25 s)

**Terminal 2**:
```bash
make dlq
```

Say: *"The same dead letter queue read from code: each failed message
decoded, with the reason and the original offset — nothing is lost, and it
can be investigated or reprocessed later."*

### 3:10 — Consumer summary (15 s)

**Terminal 1**: press `Ctrl+C`.

Say: *"Stopping the consumer prints a summary: eight orders processed, two
of them recovered after a retry, two sent to the dead letter queue, and the
final running average."*

### 3:25 — Git history (25 s)

Show: the GitHub **Commits** page.

Say: *"The project was built step by step in Git — one commit per feature:
Docker setup, Avro schema, producer, consumer, running average, retry logic,
DLQ, documentation, and then restructured into modules with tests."*

### 3:50 — Wrap-up (10 s)

Say: *"That's the full pipeline: Avro serialization, real-time aggregation,
retry logic, and a dead letter queue. Thank you."*

Stop recording.

## Optional extras (only if you have time left)

- **Poison message** — even a message that isn't valid Avro goes to the DLQ
  instead of crashing the consumer. With the consumer running, in Terminal 2:
  ```bash
  make producer ARGS="--poison"
  ```
  Terminal 1 shows a red `DLQ #poison` line, and `make dlq` shows it with the
  reason `decode error: ...`.

## Recording tips

- Any screen recorder works (OBS Studio, or your desktop's built-in one).
- Record the two terminals side by side; switch to the image/editor/browser
  only for the short "Show:" moments.
- If something goes wrong mid-recording, just stop, run the "Before you
  press record" steps again, and start over — it's a 5-minute take.
