package outbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRetryBackoffDeadLetterReplay covers the failure path: exponential
// backoff between attempts, dead-lettering after MaxAttempts, querying the
// dead-letter set, and replaying without double-applying side effects.
func TestRetryBackoffDeadLetterReplay(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()
	rec := newRecorder()

	boom := errors.New("downstream unavailable")
	rec.fail = func(*Message) error { return boom }

	cfg := fastConfig()
	cfg.MaxAttempts = 3
	cfg.BackoffJitter = 0 // deterministic backoff for timing assertions
	ob, err := New(store, rec, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var id string
	// Poison message plus a successor in the same stream.
	err = ob.Write(ctx, func(tx Tx) error {
		m := &Message{Stream: "s", Topic: "evt"}
		if err := tx.Enqueue(ctx, m); err != nil {
			return err
		}
		id = m.ID
		return tx.Enqueue(ctx, &Message{Stream: "s", Topic: "evt"})
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := ob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ob.Stop(context.Background())

	// Exhausts exactly MaxAttempts, then lands in the dead-letter set.
	waitFor(t, "message dead", func() bool {
		p, err := ob.StreamProgress(ctx, "s")
		return err == nil && p.Dead == 1
	})
	if n := rec.attemptsFor(id); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}

	// Backoff: gaps between attempts grow (base 20ms, then 40ms).
	ts := rec.attemptTimes(id)
	if len(ts) != 3 {
		t.Fatalf("recorded %d attempt times, want 3", len(ts))
	}
	if gap := ts[1].Sub(ts[0]); gap < 15*time.Millisecond {
		t.Fatalf("first retry gap %s below backoff floor", gap)
	}
	if gap := ts[2].Sub(ts[1]); gap < 30*time.Millisecond {
		t.Fatalf("second retry gap %s below backoff floor", gap)
	}

	// Dead letter is queryable and carries the failure reason.
	dead, err := ob.DeadLetters(ctx, "s", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != id || dead[0].LastError == "" {
		t.Fatalf("dead letters: %+v", dead)
	}

	// The successor stays blocked behind the dead head (strict ordering).
	p, err := ob.StreamProgress(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if p.Pending != 1 || p.Delivered != 0 {
		t.Fatalf("successor should be blocked: %+v", p)
	}

	// Replaying a live message is rejected.
	if err := ob.Replay(ctx, "no-such-id"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replay unknown: %v", err)
	}

	// Fix the downstream and replay: the stream unblocks in order.
	rec.fail = nil
	if err := ob.Replay(ctx, id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "replay delivered", func() bool { return len(rec.delivered()) == 2 })

	got := rec.delivered()
	if got[0].ID != id {
		t.Fatalf("replayed message must deliver before its successor")
	}
	if got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("order broken after replay: seqs %d, %d", got[0].Seq, got[1].Seq)
	}

	// Redelivery of the replayed message (e.g. after another crash) must not
	// re-apply business side effects: the consumer dedups by ID.
	consumer := &idempotentConsumer{seen: map[string]int{}}
	consumer.Receive(got[0])
	consumer.Receive(got[0]) // duplicate redelivery
	if n := consumer.applied[got[0].ID]; n != 1 {
		t.Fatalf("side effect applied %d times, want 1", n)
	}
}
