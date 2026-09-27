package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMaxPendingBackpressure verifies the store-level pending cap rejects
// writes with ErrOutboxFull (rolling the business transaction back too) and
// accepts them again once the backlog drains.
func TestMaxPendingBackpressure(t *testing.T) {
	store := tempStore(t, WithMaxPending(2))
	ctx := context.Background()
	if _, err := store.db.Exec(`CREATE TABLE biz (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	ob, err := New(store, rec, fastConfig())
	if err != nil {
		t.Fatal(err)
	}

	write := func(bizID, stream string) error {
		return ob.Write(ctx, func(tx Tx) error {
			if _, err := tx.Raw().(*sql.Tx).ExecContext(ctx, `INSERT INTO biz VALUES (?)`, bizID); err != nil {
				return err
			}
			return tx.Enqueue(ctx, &Message{Stream: stream, Topic: "evt"})
		})
	}

	if err := write("b1", "s1"); err != nil {
		t.Fatal(err)
	}
	if err := write("b2", "s2"); err != nil {
		t.Fatal(err)
	}
	// Third write exceeds the cap: the whole transaction rolls back.
	err = write("b3", "s3")
	if !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("want ErrOutboxFull, got %v", err)
	}
	var bizRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM biz`).Scan(&bizRows); err != nil {
		t.Fatal(err)
	}
	if bizRows != 2 {
		t.Fatalf("business row leaked from rejected tx: %d", bizRows)
	}

	// Drain the backlog; writes are accepted again.
	if err := ob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ob.Stop(context.Background())
	waitFor(t, "backlog drained", func() bool { return len(rec.delivered()) == 2 })
	if err := write("b4", "s3"); err != nil {
		t.Fatalf("write after drain: %v", err)
	}
	waitFor(t, "third delivered", func() bool { return len(rec.delivered()) == 3 })
}

// gatePublisher blocks every publish until released, tracking the maximum
// number of concurrent in-flight publishes.
type gatePublisher struct {
	gate      chan struct{}
	once      sync.Once
	cur       atomic.Int64
	max       atomic.Int64
	published atomic.Int64
}

func newGatePublisher() *gatePublisher { return &gatePublisher{gate: make(chan struct{})} }

func (g *gatePublisher) open() { g.once.Do(func() { close(g.gate) }) }

func (g *gatePublisher) Publish(ctx context.Context, msg *Message) error {
	cur := g.cur.Add(1)
	for {
		m := g.max.Load()
		if cur <= m || g.max.CompareAndSwap(m, cur) {
			break
		}
	}
	defer g.cur.Add(-1)
	select {
	case <-g.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	g.published.Add(1)
	return nil
}

// TestInFlightAndConcurrencyLimits proves claimed-but-unresolved messages
// never exceed MaxInFlight and concurrent publishes never exceed
// Concurrency, even while the downstream is completely unavailable.
func TestInFlightAndConcurrencyLimits(t *testing.T) {
	seed := func(t *testing.T, ob *Outbox, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			i := i
			if err := ob.Write(context.Background(), func(tx Tx) error {
				return tx.Enqueue(context.Background(), &Message{Stream: fmt.Sprintf("s-%d", i), Topic: "evt"})
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	blockedCfg := func() Config {
		cfg := fastConfig()
		cfg.PublishTimeout = 30 * time.Second
		cfg.LeaseDuration = time.Minute
		return cfg
	}

	t.Run("MaxInFlight caps claims", func(t *testing.T) {
		store := tempStore(t)
		gate := newGatePublisher()
		cfg := blockedCfg()
		cfg.Concurrency = 8
		cfg.MaxInFlight = 3
		ob, err := New(store, gate, cfg)
		if err != nil {
			t.Fatal(err)
		}
		seed(t, ob, 10)
		if err := ob.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer ob.Stop(context.Background())

		waitFor(t, "3 in flight", func() bool { return gate.cur.Load() == 3 })
		st, err := ob.Stats(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Claimed != 3 || st.Pending != 7 {
			t.Fatalf("in-flight cap breached: %+v", st)
		}
		gate.open()
		waitFor(t, "all delivered", func() bool { return gate.published.Load() == 10 })
		if m := gate.max.Load(); m > 3 {
			t.Fatalf("concurrent publishes reached %d, cap 3", m)
		}
	})

	t.Run("Concurrency caps parallel publishes", func(t *testing.T) {
		store := tempStore(t)
		gate := newGatePublisher()
		cfg := blockedCfg()
		cfg.Concurrency = 2
		cfg.MaxInFlight = 8
		ob, err := New(store, gate, cfg)
		if err != nil {
			t.Fatal(err)
		}
		seed(t, ob, 10)
		if err := ob.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer ob.Stop(context.Background())

		waitFor(t, "2 workers busy", func() bool { return gate.cur.Load() == 2 })
		// Nothing else gets claimed while both workers are occupied.
		time.Sleep(5 * cfg.PollInterval)
		st, err := ob.Stats(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if st.Claimed != 2 {
			t.Fatalf("claimed %d with 2 workers, want 2", st.Claimed)
		}
		gate.open()
		waitFor(t, "all delivered", func() bool { return gate.published.Load() == 10 })
		if m := gate.max.Load(); m > 2 {
			t.Fatalf("concurrent publishes reached %d, cap 2", m)
		}
	})
}
