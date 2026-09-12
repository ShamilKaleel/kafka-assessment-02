# Task List

Working through the assignment (see `Assignment.md` / `Assignment-Explained.md`)
one task at a time. Each completed task gets its own commit before moving to
the next.

## Commit convention

- One commit per completed task — don't bundle multiple tasks into one commit.
- Commit message: **short, one line, imperative, descriptive.** No AI
  co-author trailer, no multi-line body — just the one line.
  Examples: `Add order.avsc Avro schema`, `Implement producer with Avro encoding`.
- Commits use the git identity already configured for this repo
  (`ShamilKaleel <shamilkaleel81@gmail.com>`) — nothing extra to set up.
- Check off the task below (`- [x]`) as part of the same commit that
  completes it.

## Tasks

- [x] **0. Planning docs** — assignment explainer (`Assignment-Explained.md`)
  and the 4 Kafka diagrams in `diagrams/`.
  Commit: `Add step-by-step assignment explainer with Kafka diagrams`

- [x] **1. Local Kafka via Docker Compose** — add `docker-compose.yml`
  (single-broker Kafka in KRaft mode, no separate Zookeeper) so
  `docker compose up` brings up a working broker for dev and the live demo.
  Commit: `Add docker-compose setup for local Kafka`

- [x] **2. Avro schema** — add `order.avsc` defining the order message
  (`orderId`: string, `product`: string, `price`: float), matching the
  assignment's schema table.
  Commit: `Add order.avsc Avro schema`

- [x] **3. Minimal producer** — Python script that generates random orders
  and sends them Avro-encoded to the `orders` topic.
  Commit: `Implement producer with Avro encoding`

- [x] **4. Minimal consumer** — Python script that reads from `orders`,
  decodes with the same schema, and prints each order.
  Commit: `Implement consumer with Avro decoding`

- [x] **5. Real-time running average** — consumer recalculates and prints the
  running average price after every successfully processed order.
  Commit: `Add running average calculation to consumer`

- [x] **6. Retry logic** — wrap message processing so temporary failures are
  retried a bounded number of times before being treated as permanent.
  Commit: `Add retry logic for temporary processing failures`

- [x] **7. Dead Letter Queue (DLQ)** — add an `orders-dlq` topic and route
  permanently-failed messages to it once retries are exhausted.
  Commit: `Add DLQ routing for permanently failed messages`

- [x] **8. README** — how to start Kafka, run the producer/consumer, and a
  script for demoing the retry → DLQ path live.
  Commit: `Add README with run and demo instructions`

- [x] **9. Final review** — read through everything once more and rehearse
  the live demo end-to-end.
  Commit: `Final cleanup before submission`

## Stack

- **Language:** Python
- **Kafka:** Docker Compose, KRaft mode (no Zookeeper)
