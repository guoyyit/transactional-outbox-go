# transactional-outbox-go

Transactional outbox for Go: persist business state changes and outbound
messages in **one local transaction**, then deliver them to downstream
systems reliably in the background.

The problem it solves: if you update your database and then publish a
message as two separate steps, any failure in between leaves the two
inconsistent — downstream either misses events or sees events for rolled-back
state. With this library, both writes commit or roll back atomically, and an
independent delivery pipeline guarantees the committed messages arrive.

```go
store, _ := outbox.OpenSQLite("data/outbox.db", outbox.WithMaxPending(100_000))
ob, _ := outbox.New(store, myPublisher, outbox.Config{})
ob.Start(ctx)
defer ob.Stop(context.Background())

// Business write + message enqueue commit or roll back together.
err := ob.Write(ctx, func(tx outbox.Tx) error {
    sqlTx := tx.Raw().(*sql.Tx)              // business writes on the same tx
    if _, err := sqlTx.ExecContext(ctx, `UPDATE accounts SET balance = balance - ? WHERE id = ?`, 100, from); err != nil {
        return err                           // rollback: message never visible
    }
    return tx.Enqueue(ctx, &outbox.Message{  // staged in the same transaction
        Stream:  "account:" + from,          // per-stream ordering scope
        Topic:   "account.debited",
        Payload: payload,
    })
})
```

## Delivery semantics

* **At-least-once transport.** A message is marked `delivered` only after
  `Publisher.Publish` returns success. Crashes between publish and the
  marking, or lease expiry while a worker is stuck, can cause a message to be
  published more than once.
* **Idempotency key.** Every message carries a unique `Message.ID`
  (auto-assigned UUIDv4 at enqueue, or set your own). Downstream consumers
  **must** deduplicate by `Message.ID` to achieve effectively-once side
  effects. `DedupPublisher` provides in-process idempotency for publishers.
* **Per-stream ordering.** Each message gets a monotonically increasing
  `Message.Seq` within its `Stream`, assigned inside the writing transaction
  (i.e. commit order). Only the head (lowest unresolved seq) of a stream is
  ever delivered, so messages of one stream arrive in commit order. Streams
  are fully independent: one stalled stream never blocks others.
* **No intermediate state leaks.** Uncommitted messages are invisible to the
  dispatcher; a rolled-back transaction leaves no trace in the outbox.

## Crash recovery

All dispatcher state lives in the database:

* Claimed-but-unresolved messages carry a **lease**. A background reclaimer
  returns messages with expired leases to `pending`, so a worker dying
  mid-delivery never loses a message.
* `Outbox.Start` additionally resets **all** claimed messages to `pending`,
  recovering anything a previously crashed process held. (Deploy one
  dispatcher process per store, or rely on lease expiry between instances.)
* Messages already marked `delivered` are never dispatched again.
* Each claim carries a unique ownership token (`Message.ClaimOwner`), so a
  stale worker whose lease expired and was re-claimed cannot finalize
  (deliver/reschedule/kill) the message.

## Retry, backoff and dead letters

* Failed publishes are retried with exponential backoff:
  `BaseBackoff * 2^(attempt-1)`, capped at `MaxBackoff`, randomized by
  ±`BackoffJitter`.
* After `MaxAttempts` attempts the message moves to the **dead-letter set**
  (`status = dead`). A dead message blocks its stream (strict ordering) but
  no other stream.
* Inspect with `Outbox.DeadLetters(ctx, stream, limit, offset)`; re-deliver
  with `Outbox.Replay(ctx, id)`, which resets the attempt budget and keeps
  the original ID and stream position — downstream dedup by ID prevents
  re-applying side effects, and ordering is preserved.

## Bounded resource usage

* `MaxInFlight` caps claimed-but-unresolved messages process-wide; the
  poller stops claiming when the cap is reached. Memory stays bounded even
  if the downstream is down for hours.
* `Concurrency` caps parallel `Publish` calls; `BatchSize` caps claims per
  poll cycle.
* `WithMaxPending(n)` (store option) gives writers backpressure:
  `Tx.Enqueue` fails with `ErrOutboxFull` once `n` undelivered messages
  exist, rolling the business transaction back too. The outbox cannot grow
  without bound.

## Cleanup

Delivered messages older than `Retention` are deleted in bounded batches
every `CleanupInterval` (and on demand via `Outbox.Cleanup`). Cleanup only
ever touches `delivered` rows — `pending`, `claimed` and `dead` messages are
never removed — and is safe to run concurrently with delivery: claims are
atomic conditional updates, so no dangling or duplicate delivery results.
Set `Retention <= 0` to disable automatic cleanup.

## Multi-tenant streams

Business parties write to independent streams (e.g. `Stream: "order:123"`)
concurrently; writes are serialized only by SQLite's single-writer nature
(short, in-process mutex). Delivery, retry and cleanup progress are tracked
per stream: `Outbox.StreamProgress(ctx, stream)` reports pending / claimed /
delivered / dead counts, the next sequence number and the last delivered
sequence; `Outbox.Stats` gives the store-wide view.

## Configuration

### `outbox.Config` (dispatcher)

| Field             | Default              | Meaning |
|-------------------|----------------------|---------|
| `InstanceID`      | `<host>-<pid>-<rand>`| Identity prefix of claim tokens. |
| `PollInterval`    | `100ms`              | Delay between claim polls. |
| `BatchSize`       | `64`                 | Max messages claimed per poll cycle; also the cleanup delete batch. |
| `Concurrency`     | `8`                  | Max parallel `Publish` calls. |
| `MaxInFlight`     | `256`                | Max claimed-but-unresolved messages process-wide. |
| `LeaseDuration`   | `30s`                | How long a claim is owned before it may be reclaimed. Must exceed `PublishTimeout`. |
| `PublishTimeout`  | `10s`                | Context deadline for one `Publish` call. |
| `MaxAttempts`     | `8`                  | Delivery attempts before dead-lettering. |
| `BaseBackoff`     | `200ms`              | Delay after the first failed attempt. |
| `MaxBackoff`      | `30s`                | Cap on the exponential retry delay. |
| `BackoffJitter`   | `0.2`                | Randomization factor of retry delays, in `[0,1)`. |
| `ReclaimInterval` | `5s`                 | How often expired claims are returned to pending. |
| `CleanupInterval` | `1m`                 | How often delivered messages are pruned. |
| `Retention`       | `24h`                | How long delivered messages are kept. `<= 0` disables cleanup. |

### Store options (`OpenSQLite`)

| Option               | Default | Meaning |
|----------------------|---------|---------|
| `WithMaxPending(n)`  | `0` (unlimited) | Hard cap on undelivered (pending+claimed) messages; `Tx.Enqueue` fails with `ErrOutboxFull` when reached. |

`OpenSQLite` accepts a file path, `:memory:`, or a full `file:` DSN. The
file-backed mode enables WAL; a single process should own writes to the
database file (per-stream sequence assignment is protected by an in-process
write mutex and the `UNIQUE(stream, seq)` constraint).

## Consistency guarantees — summary

| Guarantee | Mechanism |
|-----------|-----------|
| Business write and message insert are atomic | Single local transaction (`Outbox.Write`) |
| Committed messages are never lost | Durable outbox + lease-based claims + startup reset |
| Already-delivered messages are never re-dispatched | `delivered` status is terminal; cleanup only removes delivered rows |
| Duplicate side effects are preventable | Unique `Message.ID` idempotency key + `DedupPublisher` / consumer dedup |
| Per-stream commit order | Transactional per-stream `Seq` + head-of-line claiming |
| Bounded memory and outbox growth | `MaxInFlight`, `BatchSize`, `Concurrency`, `WithMaxPending` |
| Cleanup never interferes with delivery | Atomic conditional updates; cleanup touches only `delivered` rows |

## Testing

```
go test -race ./...
```

Covers transaction rollback, crash recovery, duplicate delivery, retry and
dead letters, replay, lease expiry, stale-claim fencing, cleanup running
concurrently with delivery, backpressure, and concurrency/in-flight limits.
