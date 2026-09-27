package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/transactional-outbox-go/outbox"
)

// alwaysFailPublisher fails every publish and records attempts per ID.
type alwaysFailPublisher struct {
	mu    sync.Mutex
	count map[string]int
}

func (p *alwaysFailPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	p.mu.Lock()
	p.count[msg.ID]++
	p.mu.Unlock()
	return errors.New("permanently down")
}

func (p *alwaysFailPublisher) attempts(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count[id]
}

func dlqConfig() outbox.Config {
	c := outbox.DefaultConfig()
	c.MaxAttempts = 3
	c.LeaseDuration = time.Minute
	c.Backoff = outbox.ExponentialBackoff{Base: time.Second, Max: time.Minute, Factor: 2}
	return c
}

// pump advances the fake clock through backoff windows and keeps claiming until
// the message reaches the given status or attempts run out.
func pump(t *testing.T, d *outbox.Dispatcher, clk *fakeClock, ctx context.Context, maxTicks int) {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		d.RunOnce(ctx)
		d.WaitIdle()
		clk.Advance(2 * time.Second) // exceed backoff deadlines
	}
}

// TestRetryThenDead: failures are retried with backoff, and after MaxAttempts
// the message lands in the queryable DLQ and blocks later stream messages.
func TestRetryThenDead(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	pub := &alwaysFailPublisher{count: map[string]int{}}
	st, ob := newTestOutbox(t, pub, dlqConfig(), clk)
	d := ob.Dispatcher()

	enqueueN(ctx, t, ob, "s", 2)
	pump(t, d, clk, ctx, 6)

	if n := pub.attempts("msg-1"); n != 3 {
		t.Fatalf("msg-1 attempts = %d, want 3", n)
	}
	if n := pub.attempts("msg-2"); n != 0 {
		t.Fatalf("msg-2 must be blocked behind dead head, attempts = %d", n)
	}
	m, err := st.Get(ctx, "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != outbox.StatusDead {
		t.Fatalf("msg-1 status = %v, want dead", m.Status)
	}
	if m.LastError == "" {
		t.Fatal("dead message should record last error")
	}

	dead, err := ob.DLQ().List(ctx, "s", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].ID != "msg-1" {
		t.Fatalf("DLQ list = %+v", dead)
	}
	deadAll, err := ob.DLQ().List(ctx, "", 10, 0)
	if err != nil || len(deadAll) != 1 {
		t.Fatalf("DLQ list all = %+v err=%v", deadAll, err)
	}
}

// TestDLQReplayRedelivers: replaying the dead head reschedules it; a recovered
// downstream then delivers it and the blocked follower in order. Business side
// effects are not re-executed (only Publish is called again).
func TestDLQReplayRedelivers(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := dlqConfig()

	failing := &alwaysFailPublisher{count: map[string]int{}}
	st, ob := newTestOutbox(t, failing, cfg, clk)
	d := ob.Dispatcher()
	enqueueN(ctx, t, ob, "s", 2)
	pump(t, d, clk, ctx, 6)

	// Recover downstream: build a new dispatcher with a healthy publisher.
	okPub := newRecordingPublisher()
	d2 := outbox.NewDispatcher(st, okPub, cfg, clk)

	n, err := ob.DLQ().ReplayAll(ctx, "s")
	if err != nil || n != 1 {
		t.Fatalf("ReplayAll = %d, %v", n, err)
	}
	m, _ := st.Get(ctx, "msg-1")
	if m.Status != outbox.StatusPending || m.Attempt != 0 {
		t.Fatalf("after replay status=%v attempt=%d", m.Status, m.Attempt)
	}

	for i := 0; i < 10 && len(okPub.delivered()) < 2; i++ {
		d2.RunOnce(ctx)
		d2.WaitIdle()
	}
	if got := okPub.seqFor("s"); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("redelivery order = %v", got)
	}
	// Original failing attempts stay at 3; only the healthy relay published.
	if failing.attempts("msg-2") != 0 {
		t.Fatalf("follower was delivered while head was dead: %d", failing.attempts("msg-2"))
	}
}

// TestCleanerOnlyRemovesOldDelivered: pending/retrying/dead rows survive
// cleanup regardless of age; only old delivered rows are purged.
func TestCleanerSafety(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := dlqConfig()
	cfg.Retention = time.Hour
	pub := &alwaysFailPublisher{count: map[string]int{}}
	st, ob := newTestOutbox(t, pub, cfg, clk)

	// stream dead: one message driven to DLQ
	enqueueN(ctx, t, ob, "dead", 1)
	pump(t, ob.Dispatcher(), clk, ctx, 6)

	// stream ok: delivered messages
	goodPub := newRecordingPublisher()
	dGood := outbox.NewDispatcher(st, goodPub, cfg, clk)
	enqueueN(ctx, t, ob, "ok", 2)
	for i := 0; i < 6 && len(goodPub.delivered()) < 2; i++ {
		dGood.RunOnce(ctx)
		dGood.WaitIdle()
	}

	// stream backoff: a pending message not yet due
	enqueueN(ctx, t, ob, "pending", 1)

	// Age everything far beyond retention.
	clk.Advance(48 * time.Hour)

	removed, err := ob.Cleaner().PurgeOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Fatalf("cleaner removed %d, want 2 delivered rows", removed)
	}
	if m, _ := st.Get(ctx, "msg-1"); m == nil || m.Status != outbox.StatusDead {
		t.Fatalf("dead message must not be cleaned: %+v", m)
	}
	if m, _ := st.Get(ctx, "msg-4"); m == nil || m.Status != outbox.StatusPending {
		t.Fatalf("pending message must not be cleaned: %+v", m)
	}
	// Re-clean is a no-op.
	if n, _ := ob.Cleaner().PurgeOnce(ctx); n != 0 {
		t.Fatalf("second purge removed %d rows", n)
	}
}

// TestCleanerConcurrentWithDelivery never loses an active message nor
// redelivers a deleted one; every committed message ends delivered exactly.
func TestCleanerConcurrentWithDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clk := newFakeClock()
	cfg := fastConfig()
	cfg.Retention = 100 * time.Millisecond
	pub := newRecordingPublisher()
	_, ob := newTestOutbox(t, pub, cfg, clk)
	ob.Start(ctx)

	const writers, per = 4, 25
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			stream := streamName(w)
			for i := 0; i < per; i++ {
				if err := ob.Run(ctx, func(tx outbox.Tx) error {
					_, e := tx.Enqueue(ctx, stream, "evt", []byte("x"))
					return e
				}); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(pub.delivered()) >= writers*per {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ob.Stop()

	if got := len(pub.delivered()); got != writers*per {
		t.Fatalf("delivered %d, want %d", got, writers*per)
	}
	// Exactly-once downstream acceptance: every ID appears in delivered once.
	seen := map[string]int{}
	for _, id := range pub.delivered() {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s accepted %d times", id, n)
		}
	}
	// Per-stream order preserved.
	for w := 0; w < writers; w++ {
		seqs := pub.seqFor(streamName(w))
		if len(seqs) != per {
			t.Fatalf("stream %s got %d deliveries", streamName(w), len(seqs))
		}
		for i, s := range seqs {
			if s != int64(i+1) {
				t.Fatalf("stream %s order broken: %v", streamName(w), seqs)
			}
		}
	}
}

func streamName(w int) string {
	return "stream-" + string(rune('a'+w))
}

// TestBackpressureRejectsWhenFull: when MaxOutboxSize is reached, the whole
// transaction fails with ErrOutboxFull (business write included) and the
// outbox never grows unbounded; after draining, writes succeed again.
func TestBackpressureRejectsWhenFull(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := fastConfig()
	cfg.MaxOutboxSize = 3
	pub := newRecordingPublisher()
	st, ob := newTestOutbox(t, pub, cfg, clk)

	for i := 0; i < 3; i++ {
		enqueueN(ctx, t, ob, "s", 1)
	}
	size, _ := st.Size(ctx)
	if size != 3 {
		t.Fatalf("size = %d, want 3", size)
	}
	err := ob.Run(ctx, func(tx outbox.Tx) error {
		if err := tx.Put(ctx, "biz/overflow", []byte("x")); err != nil {
			return err
		}
		_, e := tx.Enqueue(ctx, "s", "t", []byte("x"))
		return e
	})
	if !errors.Is(err, outbox.ErrOutboxFull) {
		t.Fatalf("want ErrOutboxFull, got %v", err)
	}
	if _, ok := st.KVGet("biz/overflow"); ok {
		t.Fatal("business write from rejected transaction must roll back")
	}
	if size, _ := st.Size(ctx); size != 3 {
		t.Fatalf("outbox grew beyond bound: %d", size)
	}

	// Drain two messages and verify writes are admitted again.
	d := ob.Dispatcher()
	d.RunOnce(ctx)
	d.WaitIdle()
	clk.Advance(time.Second)
	d.RunOnce(ctx)
	d.WaitIdle()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := ob.Run(ctx, func(tx outbox.Tx) error {
			_, e := tx.Enqueue(ctx, "s", "t", []byte("x"))
			return e
		}); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if size, _ := st.Size(ctx); size != 2 {
		t.Fatalf("after drain+write size = %d, want 2 (1 active + 1 new)", size)
	}
}

// TestIndependentStreamsDoNotBlockEachOther: a dead message on stream a does
// not stop stream b from fully draining; per-stream stats distinguish progress.
func TestIndependentStreamsAndStats(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := dlqConfig()
	// stream a fails forever, stream b always succeeds: use a routing publisher.
	var mu sync.Mutex
	attempts := map[string]int{}
	pub := outbox.PublisherFunc(func(ctx context.Context, m *outbox.Message) error {
		mu.Lock()
		defer mu.Unlock()
		attempts[m.ID]++
		if m.Stream == "a" {
			return errors.New("a down")
		}
		return nil
	})
	st, ob := newTestOutbox(t, pub, cfg, clk)
	d := ob.Dispatcher()
	enqueueN(ctx, t, ob, "a", 1)
	enqueueN(ctx, t, ob, "b", 3)
	pump(t, d, clk, ctx, 8)

	ma, _ := st.Get(ctx, "msg-1")
	if ma.Status != outbox.StatusDead {
		t.Fatalf("stream a head = %v, want dead", ma.Status)
	}
	mb, _ := st.Get(ctx, "msg-4")
	if mb == nil || mb.Status != outbox.StatusDelivered {
		t.Fatalf("stream b tail = %+v, want delivered", mb)
	}
	stats, _ := st.StreamStats(ctx)
	byStream := map[string]outbox.StreamStat{}
	for _, s := range stats {
		byStream[s.Stream] = s
	}
	if byStream["a"].Dead != 1 {
		t.Fatalf("stats[a] = %+v", byStream["a"])
	}
	if byStream["b"].Delivered != 3 {
		t.Fatalf("stats[b] = %+v", byStream["b"])
	}
}
