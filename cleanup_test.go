package outbox

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCleanupSkipsNonDelivered verifies cleanup removes only delivered
// messages past retention — never pending, claimed, or dead ones.
func TestCleanupSkipsNonDelivered(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()

	cfg := fastConfig()
	cfg.Retention = time.Hour
	ob, err := New(store, newRecorder(), cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Seed one message per state.
	seed := func(stream string) string {
		var id string
		err := ob.Write(ctx, func(tx Tx) error {
			m := &Message{Stream: stream, Topic: "evt"}
			if err := tx.Enqueue(ctx, m); err != nil {
				return err
			}
			id = m.ID
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	oldDelivered := seed("a")
	recentDelivered := seed("b")
	stuckClaimed := seed("c")
	dead := seed("d")
	pending := seed("e")

	now := time.Now()
	// Claim the four seeded heads of streams a-d (e stays pending).
	claimed, err := store.ClaimDue(ctx, now, 4, "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 4 {
		t.Fatalf("claimed %d, want 4", len(claimed))
	}
	// Two delivered: one older than retention, one fresh.
	if ok, err := store.MarkDelivered(ctx, oldDelivered, "owner", now.Add(-2*time.Hour)); !ok || err != nil {
		t.Fatalf("mark old delivered: %v %v", ok, err)
	}
	if ok, err := store.MarkDelivered(ctx, recentDelivered, "owner", now); !ok || err != nil {
		t.Fatalf("mark recent delivered: %v %v", ok, err)
	}
	// One dead, one claimed-with-live-lease, one pending.
	if err := store.MarkDead(ctx, dead, "owner", "boom"); err != nil {
		t.Fatal(err)
	}
	// stuckClaimed stays claimed (lease still valid), pending stays pending.

	n, err := ob.Cleanup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("cleanup removed %d, want 1 (only the old delivered)", n)
	}

	st, err := ob.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Delivered != 1 || st.Claimed != 1 || st.Dead != 1 || st.Pending != 1 {
		t.Fatalf("cleanup touched non-expired rows: %+v", st)
	}
	for _, id := range []string{recentDelivered, stuckClaimed, dead, pending} {
		var cnt int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM outbox_messages WHERE id = ?`, id).Scan(&cnt); err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Fatalf("message %s wrongly removed", id)
		}
	}
}

// TestCleanupConcurrentWithDelivery runs the janitor concurrently with an
// active dispatcher: every message must be delivered exactly once, and no
// claim may dangle.
func TestCleanupConcurrentWithDelivery(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()
	rec := newRecorder()

	cfg := fastConfig()
	cfg.Retention = time.Millisecond // delivered rows age out almost immediately
	cfg.CleanupInterval = 5 * time.Millisecond
	// Long lease keeps the reclaimer out of this test: it isolates the
	// cleanup-vs-delivery interaction. (Lease-expiry redelivery is covered
	// by TestLeaseExpiryReclaimsStuckMessages and TestStaleClaimCannotFinalize.)
	cfg.LeaseDuration = time.Minute
	ob, err := New(store, rec, cfg)
	if err != nil {
		t.Fatal(err)
	}

	const total = 300
	for i := 0; i < total; i++ {
		i := i
		if err := ob.Write(ctx, func(tx Tx) error {
			return tx.Enqueue(ctx, &Message{
				Stream: fmt.Sprintf("s-%d", i%10), // 10 streams
				Topic:  "evt",
			})
		}); err != nil {
			t.Fatal(err)
		}
	}

	if err := ob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ob.Stop(context.Background())

	// Concurrent manual cleanup hammering the store alongside the janitor.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := ob.Cleanup(ctx); err != nil {
				t.Errorf("cleanup: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	waitFor(t, "all delivered", func() bool { return len(rec.delivered()) == total })
	wg.Wait()

	// Exactly-once at the publisher: no ID delivered twice.
	seen := map[string]int{}
	for _, m := range rec.delivered() {
		seen[m.ID]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s delivered %d times under concurrent cleanup", id, n)
		}
	}

	// No dangling claims remain; rows are either delivered or already pruned.
	st, err := ob.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Pending != 0 || st.Claimed != 0 {
		t.Fatalf("dangling work after cleanup: %+v", st)
	}
}
