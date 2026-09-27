package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/transactional-outbox-go/outbox"
	"github.com/guoyyit/transactional-outbox-go/store/memory"
)

// fakeClock is a controllable clock shared by the outbox and memory store.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// noopPublisher never fails.
func noopPublisher() outbox.Publisher {
	return outbox.PublisherFunc(func(ctx context.Context, msg *outbox.Message) error {
		return nil
	})
}

func newTestOutbox(t *testing.T, pub outbox.Publisher, cfg outbox.Config, clk *fakeClock) (*memory.Store, *outbox.DefaultOutbox) {
	t.Helper()
	st := memory.New().WithClock(clk.Now)
	ob := outbox.New(st, pub,
		outbox.WithConfig(cfg),
		outbox.WithClock(clk),
		outbox.WithIDGenerator(&outbox.SequenceIDGenerator{}),
	)
	return st, ob
}

func fastConfig() outbox.Config {
	c := outbox.DefaultConfig()
	c.PollInterval = 10 * time.Millisecond
	return c
}

// TestAtomicCommit verifies business write + message are visible together.
func TestAtomicCommit(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	st, ob := newTestOutbox(t, noopPublisher(), fastConfig(), clk)

	err := ob.Run(ctx, func(tx outbox.Tx) error {
		if err := tx.Put(ctx, "order/1", []byte("PAID")); err != nil {
			return err
		}
		if _, err := tx.Enqueue(ctx, "orders", "order.paid", []byte(`{"id":1}`)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if v, ok := st.KVGet("order/1"); !ok || string(v) != "PAID" {
		t.Fatalf("business data not committed: %q %v", v, ok)
	}
	stats, err := st.StreamStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 || stats[0].Pending != 1 {
		t.Fatalf("message not committed atomically: %+v", stats)
	}
}

// TestAtomicRollback verifies neither business write nor message survives a
// rollback at any stage of the function.
func TestAtomicRollback(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	st, ob := newTestOutbox(t, noopPublisher(), fastConfig(), clk)

	boom := errors.New("business rule rejected")

	// Failure before enqueue.
	err := ob.Run(ctx, func(tx outbox.Tx) error {
		if err := tx.Put(ctx, "k1", []byte("v1")); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	// Failure after enqueue.
	err = ob.Run(ctx, func(tx outbox.Tx) error {
		if err := tx.Put(ctx, "k2", []byte("v2")); err != nil {
			return err
		}
		if _, err := tx.Enqueue(ctx, "s", "t", []byte("x")); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	// Failure after multiple business writes.
	err = ob.Run(ctx, func(tx outbox.Tx) error {
		_ = tx.Put(ctx, "k3", []byte("v3"))
		_ = tx.Delete(ctx, "k2") // even deletion must roll back (but k2 never existed)
		_, _ = tx.Enqueue(ctx, "s", "t", []byte("y"))
		_ = tx.Put(ctx, "k4", []byte("v4"))
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}

	for _, k := range []string{"k1", "k2", "k3", "k4"} {
		if _, ok := st.KVGet(k); ok {
			t.Fatalf("key %s survived rollback", k)
		}
	}
	size, err := st.Size(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatalf("messages survived rollback: active=%d", size)
	}
}

// TestCommitNotObservableUntilCommit: dispatcher never sees a message whose
// transaction is still open (simulated by blocking fn until signal).
func TestNoIntermediateStateObserved(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	st, ob := newTestOutbox(t, noopPublisher(), fastConfig(), clk)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- ob.Run(ctx, func(tx outbox.Tx) error {
			_ = tx.Put(ctx, "k", []byte("v"))
			_, _ = tx.Enqueue(ctx, "s", "t", []byte("z"))
			close(entered)
			<-proceed
			return nil
		})
	}()
	<-entered

	// While the transaction is open, nothing is visible to other readers.
	if size, _ := st.Size(ctx); size != 0 {
		t.Fatalf("intermediate message visible, size=%d", size)
	}
	if _, ok := st.KVGet("k"); ok {
		t.Fatalf("intermediate business write visible")
	}
	// The dispatcher must also find nothing to claim.
	if msgs, _ := st.ClaimDue(ctx, clk.Now(), time.Second, 10); len(msgs) != 0 {
		t.Fatalf("dispatcher observed uncommitted messages: %d", len(msgs))
	}

	close(proceed)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if size, _ := st.Size(ctx); size != 1 {
		t.Fatalf("committed message not visible, size=%d", size)
	}
}
