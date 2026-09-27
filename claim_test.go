package outbox

import (
	"context"
	"testing"
	"time"
)

// TestStaleClaimCannotFinalize proves that once a message's lease expired
// and it was reclaimed by a new claim, the stale claim token can no longer
// mark it delivered, reschedule it, or kill it.
func TestStaleClaimCannotFinalize(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()

	var id string
	err := store.RunInTx(ctx, func(tx Tx) error {
		m := &Message{Stream: "s", Topic: "evt"}
		if err := tx.Enqueue(ctx, m); err != nil {
			return err
		}
		id = m.ID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Claim with a token that expires immediately.
	stale, err := store.ClaimDue(ctx, time.Now(), 1, "token-stale", time.Nanosecond)
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %d", err, len(stale))
	}
	time.Sleep(2 * time.Millisecond)
	if n, err := store.ReclaimExpired(ctx, time.Now()); err != nil || n != 1 {
		t.Fatalf("reclaim: %d %v", n, err)
	}
	// Re-claimed under a fresh token.
	fresh, err := store.ClaimDue(ctx, time.Now(), 1, "token-fresh", time.Minute)
	if err != nil || len(fresh) != 1 {
		t.Fatalf("re-claim: %v %d", err, len(fresh))
	}

	// The stale token is powerless now.
	if ok, err := store.MarkDelivered(ctx, id, stale[0].ClaimOwner, time.Now()); err != nil || ok {
		t.Fatalf("stale mark delivered: ok=%v err=%v", ok, err)
	}
	if err := store.Reschedule(ctx, id, stale[0].ClaimOwner, time.Now(), "x"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDead(ctx, id, stale[0].ClaimOwner, "x"); err != nil {
		t.Fatal(err)
	}
	p, err := store.StreamProgress(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if p.Claimed != 1 || p.Delivered != 0 || p.Dead != 0 {
		t.Fatalf("stale token mutated state: %+v", p)
	}

	// The fresh token finalizes normally.
	if ok, err := store.MarkDelivered(ctx, id, fresh[0].ClaimOwner, time.Now()); err != nil || !ok {
		t.Fatalf("fresh mark delivered: ok=%v err=%v", ok, err)
	}
}
