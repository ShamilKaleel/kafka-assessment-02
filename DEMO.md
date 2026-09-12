# Demo Video Script (max 5 minutes)

A timed, step-by-step script for recording the demonstration video. Follow
it top to bottom. Each segment has the exact command to run and a short
line to say. The whole thing runs about 4 min 20 s, leaving buffer.

## Before you press record

Do all of this first — none of it needs to be on camera.

1. Fresh Kafka, so the recording starts from a clean state:
   ```bash
   docker compose down -v && docker compose up -d
   docker compose ps     # wait until kafka says "healthy" (~15 s)
   ```
2. Open **Kafka UI** at http://localhost:8080 in a browser and wait until the
   `local` cluster shows as online (it starts ~30 s after Kafka is healthy).
   Leave it on the **Topics** page.
3. Open **two terminals side by side**, both in the project folder, with a
   large font (the viewer has to read the log lines).
4. Have these ready to show:
   - `diagrams/03-system-architecture.png` (open in an image viewer)
   - `order.avsc` (open in the editor)
   - the GitHub repo's **Commits** page in another browser tab
5. Nothing running in either terminal yet.

## The recording

### 0:00 — Intro (25 s)

Show: `diagrams/03-system-architecture.png`

Say: *"This is my Kafka assignment. A producer sends order messages into
Kafka, and a consumer reads them. Every message is Avro-encoded. The
consumer keeps a running average of prices, retries temporary failures, and
sends permanently failed messages to a dead letter queue."*

### 0:25 — The Avro schema (15 s)

Show: `order.avsc`

Say: *"Each order has three fields — orderId, product, and price. This
schema file is shared by the producer and the consumer, so both sides agree
on the message format."*

### 0:40 — Normal flow with running average (55 s)

**Terminal 1** (consumer):
```bash
.venv/bin/python3 consumer.py
```
**Terminal 2** (producer), a moment later:
```bash
.venv/bin/python3 producer.py --count 8
```

Say (while orders scroll in Terminal 1): *"The producer generates random
orders and sends them Avro-encoded to the `orders` topic. The consumer
decodes each one and updates the running average price after every
message — you can see it recalculating live, one message at a time."*

(On a fresh cluster the consumer prints one `UNKNOWN_TOPIC_OR_PART` line
before the first order — that's expected; it picks the topic up by itself.)

### 1:35 — Retry logic and Dead Letter Queue (70 s)

**Terminal 1**: press `Ctrl+C` to stop the consumer, then restart it with
simulated failures switched on:
```bash
.venv/bin/python3 consumer.py --fail-rate 1.0 --max-attempts 3 --retry-delay 1
```
**Terminal 2**:
```bash
.venv/bin/python3 producer.py --count 2
```

Say: *"Nothing in this pipeline fails on its own, so `--fail-rate` injects
failures for the demo. Watch: each order fails, the consumer waits and
retries — three attempts. When the last attempt fails, the message is
treated as permanently failed and routed to the `orders-dlq` topic, and the
consumer moves on instead of crashing."*

Point at the `processing failed (attempt 1/3)` … `(3/3)` lines, then the
`routed to DLQ` line.

### 2:45 — Kafka UI (35 s)

Switch to the browser tab with **Kafka UI** (http://localhost:8080), on the
**Topics** page. Click **orders-dlq** → **Messages** tab → expand one
message.

Say: *"In the Kafka dashboard you can see both topics — `orders` with all
the orders, and `orders-dlq` with the two that failed. Opening a failed
message shows its headers: the error reason and exactly where it came from."*

(The message *value* shows as raw bytes here — that's the Avro encoding,
which the dashboard can't decode without a schema registry. The decoded
orders are shown in the terminal instead.)

### 3:20 — Inspect the DLQ from code (25 s)

**Terminal 1**: `Ctrl+C` to stop the consumer, then:
```bash
.venv/bin/python3 dlq_reader.py
```

Say: *"The same dead letter queue read from code: each failed message with
its decoded order, the failure reason, and the original offset — nothing is
lost, and it can be investigated or reprocessed later."*

### 3:45 — Git history (25 s)

Show: the GitHub **Commits** page.

Say: *"The project was built step by step in Git — one commit per feature:
Docker setup, Avro schema, producer, consumer, running average, retry logic,
DLQ, and documentation."*

### 4:10 — Wrap-up (10 s)

Say: *"That's the full pipeline: Avro serialization, real-time aggregation,
retry logic, and a dead letter queue. Thank you."*

Stop recording.

## Optional extra (only if you have time left)

Show that even an *undecodable* message goes to the DLQ instead of crashing
the consumer. With the consumer running normally (no `--fail-rate`), in
Terminal 2:
```bash
.venv/bin/python3 -c "from confluent_kafka import Producer; p = Producer({'bootstrap.servers': 'localhost:9092'}); p.produce('orders', key=b'bad', value=b'not avro'); p.flush()"
```
Terminal 1 shows `routed to DLQ key=bad`, and `dlq_reader.py` shows it with
reason `decode error: ...`.

## Recording tips

- Any screen recorder works (OBS Studio, or your desktop's built-in one).
- Record the two terminals side by side; switch to the image/editor/browser
  only for the short "Show:" moments.
- If something goes wrong mid-recording, just stop, run the "Before you
  press record" steps again, and start over — it's a 5-minute take.
