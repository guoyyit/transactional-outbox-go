package outbox

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestCrashRecovery simulates a process crash at every interesting point of
// the delivery path and verifies a restarted process continues delivery
// without loss and without re-publishing already-delivered messages.
func TestCrashRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbox.db")

	// --- Phase A: write 5 messages, deliver some, then "crash". ---
	storeA, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	obA, err := New(storeA, newRecorder(), fastConfig())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 5)
	for i := range ids {
		i := i
		err := obA.Write(ctx, func(tx Tx) error {
			m := &Message{Stream: "s", Topic: "evt", Payload: []byte{byte('a' + i)}}
			if err := tx.Enqueue(ctx, m); err != nil {
				return err
			}
			ids[i] = m.ID
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Claim the stream head, deliver it, then claim the next — exactly how
	// the dispatcher chains claims within a stream.
	first, err := storeA.ClaimDue(ctx, time.Now(), 1, "dead-instance", time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim head: %v %d", err, len(first))
	}
	// Message 1: fully delivered before the crash.
	if ok, err := storeA.MarkDelivered(ctx, first[0].ID, "dead-instance", time.Now()); err != nil || !ok {
		t.Fatalf("mark delivered: ok=%v err=%v", ok, err)
	}
	// Message 2: published downstream but never marked (crash in the ack
	// window) — it stays claimed. Messages 3-5 stay pending.
	second, err := storeA.ClaimStreamHead(ctx, "s", "dead-instance", time.Minute, time.Now())
	if err != nil || second == nil {
		t.Fatalf("claim second: %v %v", err, second)
	}
	claimed := []*Message{first[0], second}
	// Crash: close the store without any dispatcher shutdown.
	if err := storeA.Close(); err != nil {
		t.Fatal(err)
	}

	// --- Phase B: restart on the same database file. ---
	storeB, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()
	rec := newRecorder()
	obB, err := New(storeB, rec, fastConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := obB.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer obB.Stop(context.Background())

	// Everything not marked delivered is (re-)published after restart.
	waitFor(t, "remaining messages republished", func() bool { return len(rec.delivered()) == 4 })

	got := map[string]int{}
	for _, m := range rec.delivered() {
		got[m.ID]++
	}
	// The message marked delivered before the crash is never published again.
	if got[claimed[0].ID] != 0 {
		t.Fatalf("already-delivered message %s was republished", claimed[0].ID)
	}
	// The other four arrive exactly once each via the dispatcher.
	for _, id := range ids[1:] {
		if got[id] != 1 {
			t.Fatalf("message %s published %d times, want 1", id, got[id])
		}
	}

	// An idempotent consumer keyed on Message.ID sees each message once even
	// across the crash-window duplicate.
	consumer := &idempotentConsumer{seen: map[string]int{}}
	for _, m := range rec.delivered() {
		consumer.Receive(m)
	}
	consumer.Receive(&Message{ID: claimed[1].ID}) // simulated crash-window duplicate
	if n := consumer.applied[claimed[1].ID]; n != 1 {
		t.Fatalf("consumer applied crash-window message %d times, want 1", n)
	}
}

// idempotentConsumer deduplicates by Message.ID, as downstream must.
type idempotentConsumer struct {
	mu      sync.Mutex
	seen    map[string]int
	applied map[string]int
}

func (c *idempotentConsumer) Receive(m *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[m.ID]++
	if c.seen[m.ID] > 1 {
		return // duplicate delivery; do not re-apply side effects
	}
	if c.applied == nil {
		c.applied = map[string]int{}
	}
	c.applied[m.ID]++
}

// TestLeaseExpiryReclaimsStuckMessages covers the reclaim path for workers
// that die while holding a claim (no clean restart).
func TestLeaseExpiryReclaimsStuckMessages(t *testing.T) {
	ctx := context.Background()
	store := tempStore(t)

	err := store.RunInTx(ctx, func(tx Tx) error {
		return tx.Enqueue(ctx, &Message{Stream: "s", Topic: "evt"})
	})
	if err != nil {
		t.Fatal(err)
	}
	// Claim with a lease that expires immediately.
	claimed, err := store.ClaimDue(ctx, time.Now(), 1, "owner-1", time.Nanosecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %d", err, len(claimed))
	}
	time.Sleep(2 * time.Millisecond)
	n, err := store.ReclaimExpired(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("reclaimed %d, want 1", n)
	}
	// Deliverable again by a new owner.
	claimed2, err := store.ClaimDue(ctx, time.Now(), 1, "owner-2", time.Minute)
	if err != nil || len(claimed2) != 1 {
		t.Fatalf("re-claim: %v %d", err, len(claimed2))
	}
	if claimed2[0].ID != claimed[0].ID {
		t.Fatalf("reclaimed wrong message")
	}
}

// TestDedupPublisher verifies in-process idempotency of DedupPublisher.
func TestDedupPublisher(t *testing.T) {
	rec := newRecorder()
	pub := NewDedupPublisher(rec)
	ctx := context.Background()

	msg := &Message{ID: "m1", Stream: "s", Topic: "t"}
	for i := 0; i < 3; i++ {
		if err := pub.Publish(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(rec.delivered()); got != 1 {
		t.Fatalf("inner publisher saw %d messages, want 1", got)
	}

	// Failures are not deduplicated away: a failed publish may be retried.
	rec.fail = func(*Message) error { return context.DeadlineExceeded }
	bad := &Message{ID: "m2", Stream: "s", Topic: "t"}
	if err := pub.Publish(ctx, bad); err == nil {
		t.Fatal("want error")
	}
	rec.fail = nil
	if err := pub.Publish(ctx, bad); err != nil {
		t.Fatal(err)
	}
	if n := rec.attemptsFor("m2"); n != 2 {
		t.Fatalf("m2 attempted %d times, want 2", n)
	}
}
