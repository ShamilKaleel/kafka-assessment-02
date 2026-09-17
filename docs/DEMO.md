# Demo Script — one case at a time

A step-by-step script for the demonstration video. There is no clock: the
goal is to explain properly, not to be fast (about 8 minutes is fine).

The shape of the demo:

1. **Theory** — what Kafka is, and what the assignment asks for
2. **The system** — the diagram, the schema, the dashboard
3. **Four demonstrations, one at a time** — the consumer stays running
   throughout; each case is a separate producer run, so you can explain each
   result before moving on:
   - Case 1 — normal orders → running average
   - Case 2 — temporary failure → retry → recovers
   - Case 3 — permanent failure → Dead Letter Queue
   - Case 4 — poison message → Dead Letter Queue, no retries
4. **Wrap-up** — consumer summary, tests, Git history

Throughout, keep **two terminals side by side** in the project folder:
**T1** runs the consumer, **T2** runs everything else. Keep the browser with
Kafka UI on a second screen or ready to switch to.

---

## Before the demo (not on camera)

1. Start everything fresh, so offsets begin at 0 and the DLQ is empty:
   ```bash
   make reset        # stop, wipe topic data, start again
   make ps           # kafka = "healthy", kafka-init = "Exited (0)" (topics created), kafka-ui = Up
   ```
2. Open **Kafka UI** at http://localhost:8080 (it comes up ~30 s after Kafka).
   Prepare three browser tabs:
   - **Tab A**: *Topics* — the list; `orders` and `orders-dlq` already exist.
   - **Tab B**: *Topics → orders → Messages*
   - **Tab C**: *Topics → orders-dlq → Messages*

   In tabs B and C switch the message list to **Live Mode** (open the
   *Oldest First* dropdown and choose *Live Mode*; click *Submit* if it
   doesn't start). New messages will then appear by themselves while the
   producer runs.
3. Open, ready to show:
   - `docs/diagrams/07-high-level-overview.png` (the overview diagram)
   - `schemas/order.avsc` (the Avro schema)
   - the GitHub repo's **Commits** page
4. Both terminals empty, nothing running. Start recording.

---

## Part 1 — Theory

Show: `docs/diagrams/07-high-level-overview.png` (left half: producer, Avro,
broker).

Say, roughly:

> "Kafka is a messaging system. A **producer** writes messages into a named
> channel called a **topic**, and a **consumer** reads them out. The Kafka
> **broker** stores the messages and delivers them reliably; it doesn't care
> what is inside them.
>
> A topic is split into **partitions** — this project uses one partition per
> topic. Every message in a partition gets a number called the **offset**:
> 0, 1, 2, 3… The consumer remembers how far it has read using that offset,
> so if it restarts it continues where it left off.
>
> Kafka only carries **bytes**. So the producer has to turn an order into
> bytes, and the consumer has to turn the bytes back into an order. That is
> **Avro serialization**: both sides share one schema file that says exactly
> what an order looks like.
>
> The assignment asks for three behaviours on top of that: a **real-time
> running average** of prices; **retry logic** for temporary failures — try
> again instead of giving up; and a **Dead Letter Queue** — a separate topic
> where messages that fail permanently are parked instead of being lost or
> crashing the consumer."

---

## Part 2 — The system

Show: the whole of `docs/diagrams/07-high-level-overview.png`.

Say:

> "Here is the system. The producer creates random orders and Avro-encodes
> them into the `orders` topic. The consumer decodes each one and processes
> it. If processing succeeds, the running average is updated. If it fails,
> the consumer checks whether it has attempts left — up to three — and waits
> and retries. If all attempts fail, the message is sent to the `orders-dlq`
> topic with headers saying why. Either way it then commits the offset and
> fetches the next message. Underneath, Kafka UI shows the topics,
> partitions, offsets, messages and consumer lag live."

Show: `schemas/order.avsc`.

Say:

> "This is the schema: three fields — `orderId`, `product`, `price`. Both the
> producer and the consumer load this same file."

Show: Kafka UI **Tab A** (Topics).

Say:

> "Both topics are already there — `orders` and `orders-dlq` — one partition
> each, and both empty. They were created when the containers started."

Say, about the failures you are about to show:

> "Nothing in this pipeline fails on its own. To demonstrate the retry and
> DLQ behaviour, the producer can tag an order with a small header —
> `simulate-failure` — asking the consumer to simulate a temporary or a
> permanent failure. The order data itself is never changed."

---

## Part 3 — Demonstrations

### Start the consumer (once, and leave it running)

**T1:**
```bash
make consumer
```

It prints a cyan box with its configuration: topic `orders`, DLQ
`orders-dlq`, group `order-consumer-group`, attempts 3, delay 1.0 s. Then it
waits.

Say: *"The consumer is now subscribed to the `orders` topic and waiting."*

---

### Case 1 — Normal orders → running average

**T2:**
```bash
make producer ARGS="--count 5"
```

**What appears**

T2 (producer), five green lines:
```
SENT      #1001 Item5  $ 471.60  -> orders [p0 @0]
SENT      #1002 Item4  $ 490.37  -> orders [p0 @1]
...
```

T1 (consumer), five green lines:
```
RECEIVED  #1001 Item5  $ 471.60  avg $ 471.60 (n=1)  [p0 @0]
RECEIVED  #1002 Item4  $ 490.37  avg $ 480.99 (n=2)  [p0 @1]
RECEIVED  #1003 Item5  $ 175.34  avg $ 379.10 (n=3)  [p0 @2]
...
```

Say:

> "Each `SENT` line is one order, Avro-encoded and written to partition 0 —
> you can see the offset going 0, 1, 2, 3, 4. On the consumer side, each
> `RECEIVED` line is that same order decoded again, and after every single
> message the average price is recalculated — `n` is how many orders are
> included so far. That's the real-time aggregation."

Show: Kafka UI **Tab B** (`orders` → Messages, live).

Say:

> "The dashboard picked up the same five messages as they arrived. The key
> is the order id. The value looks like binary — that is the Avro encoding;
> the dashboard has no schema to decode it, which is exactly why the
> producer and consumer share the schema file."

**Result of this case:** 5 orders processed, average over 5, DLQ still empty.

---

### Case 2 — Temporary failure → retry → recovers

**T2:**
```bash
make producer ARGS="--count 3 --scenario transient --start-id 2001"
```

**What appears**

T2: each `SENT` line ends with the tag `(transient)`.

T1, for **each** of the three orders:
```
RETRY     #2001 attempt 1/3 failed: simulated downstream timeout (transient) - retrying in 1.0s
RECOVERED #2001 succeeded on attempt 2
RECEIVED  #2001 Item2  $ 145.17  avg $ 314.85 (n=6)  [p0 @5]
```

Say:

> "This order failed on the first attempt — a temporary error, like a
> service timing out. The consumer did **not** give up: it waited one second
> and tried again, and the second attempt succeeded. Notice the order is
> still counted in the running average — nothing was lost. That is the retry
> logic for temporary failures."

Show: Kafka UI **Tab B** — open one of the new messages (`2001`…) and point
at its headers: `simulate-failure: transient`.

Say:

> "Here is the tag the producer added to request the simulated failure. The
> order itself is unchanged."

Show: Kafka UI **Tab C** (`orders-dlq`) — still nothing.

Say:

> "And the dead letter queue is still empty: a temporary failure that
> recovers never goes there."

**Result of this case:** 3 more orders processed (8 total), each one retried
once, DLQ still empty.

---

### Case 3 — Permanent failure → Dead Letter Queue

**T2:**
```bash
make producer ARGS="--count 2 --scenario permanent --start-id 3001"
```

**What appears**

T2: each `SENT` line ends with `(permanent)`.

T1, for **each** of the two orders:
```
RETRY     #3001 attempt 1/3 failed: simulated validation error (permanent) - retrying in 1.0s
RETRY     #3001 attempt 2/3 failed: simulated validation error (permanent) - retrying in 1.0s
FAILED    #3001 attempt 3/3 failed: simulated validation error (permanent) - giving up
DLQ       #3001 -> orders-dlq [p0 @0]
```

Say:

> "This order fails every time — like invalid data that no retry can fix.
> Attempt one, wait, attempt two, wait, attempt three — and now there are no
> attempts left, so the consumer stops retrying and sends the message to the
> `orders-dlq` topic. Two things to notice: the running average did **not**
> change — a failed order is not counted — and the consumer kept running; it
> moved on to the next message instead of crashing."

Show: Kafka UI **Tab C** (`orders-dlq`, live) — two messages have arrived.
Open one and point at the headers.

Say:

> "Here they are in the dead letter queue. Each one keeps its original bytes
> and gets extra headers: `error` — why it failed; `original-topic` and
> `original-offset` — where it came from; and the `simulate-failure` tag."

**T2:**
```bash
make dlq
```

Say:

> "The same queue read from code: a table with each failed order decoded,
> the reason, and the original offset. Nothing is lost — these can be
> investigated or re-processed later."

**Result of this case:** 2 messages in the DLQ, running average unchanged,
consumer still running.

---

### Case 4 — Poison message → DLQ without retries

**T2:**
```bash
make producer ARGS="--poison"
```

**What appears**

T2: a red line `SENT #poison (not valid Avro) -> orders`.

T1: immediately, with no `RETRY` lines:
```
DLQ       #poison -> orders-dlq [p0 @2]
```

Say:

> "This message isn't a valid Avro order at all — bad bytes. The consumer
> can't even decode it, and retrying cannot fix bad bytes, so it goes
> straight to the dead letter queue. Again the consumer keeps running."

**T2:** `make dlq` again — the third row shows `undecodable bytes` with the
reason `decode error: …`.

**Result of this case:** 3 messages in the DLQ.

---

### Optional — everything at once

If you want to show the mix in one stream:

**T2:**
```bash
make producer ARGS="--count 10 --scenario mixed --start-id 4001"
```

10 orders in a repeating pattern (normal, normal, transient, normal,
permanent): 6 green, 2 yellow → bold-green, 2 → red DLQ, all interleaved.

---

## Part 4 — Wrap-up

### Consumer summary

**T1:** press `Ctrl+C`.

A cyan summary table appears:

| | |
|---|---|
| orders processed | 8 |
| recovered after retry | 3 |
| sent to DLQ | 3 |
| final running average | $ … |

(8 = 5 normal + 3 transient; 3 recovered = the transient ones; 3 DLQ =
2 permanent + 1 poison. Add 8 / 2 / 2 if you ran the optional mixed case.)

Say:

> "Stopping the consumer prints the summary: eight orders processed, three
> of which recovered after a retry, three sent to the dead letter queue, and
> the final running average."

### Tests

**T2:**
```bash
make test-unit
```

Say:

> "The behaviour is covered by tests: the schema matches the assignment, the
> Avro round-trip, the running average, a transient failure recovering on
> the second attempt, and a permanent failure giving up after three. There
> is also an end-to-end test against the real broker."

### Git history

Show: the GitHub **Commits** page.

Say:

> "The project was built step by step — one commit per feature: Docker
> setup, Avro schema, producer, consumer, running average, retry logic, DLQ,
> documentation, and then restructured into modules with tests."

### Closing

> "That's the full pipeline: Avro serialization, a real-time running
> average, retry logic for temporary failures, and a dead letter queue for
> permanent ones. Thank you."

Stop recording.

---

## Expected results at a glance

| Case | Command (T2) | Consumer output | Running average | DLQ |
|---|---|---|---|---|
| 1 Normal | `make producer ARGS="--count 5"` | green `RECEIVED` | updated ×5 | unchanged |
| 2 Temporary failure | `make producer ARGS="--count 3 --scenario transient --start-id 2001"` | yellow `RETRY` → bold-green `RECOVERED` → `RECEIVED` | updated ×3 | unchanged |
| 3 Permanent failure | `make producer ARGS="--count 2 --scenario permanent --start-id 3001"` | `RETRY` ×2 → red `FAILED` → red `DLQ` | unchanged | +2 |
| 4 Poison | `make producer ARGS="--poison"` | red `DLQ` immediately | unchanged | +1 |

## If something goes wrong

- Consumer shows nothing after a producer run: check `make ps` — the broker
  must be `healthy`. If the consumer was restarted with the same group it
  continues from its last offset, which is expected.
- To start the whole demo again from zero: `Ctrl+C` the consumer, then
  `make reset`, and redo the "Before the demo" steps.
- Kafka UI live mode stopped: choose *Live Mode* again and click *Submit*,
  or pick *Oldest First* to browse what is already stored.
