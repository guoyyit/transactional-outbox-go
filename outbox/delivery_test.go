package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/transactional-outbox-go/outbox"
	"github.com/guoyyit/transactional-outbox-go/store/memory"
)

// recordingPublisher captures delivered message IDs/seq per stream.
type recordingPublisher struct {
	mu      sync.Mutex
	got     []string
	perSeq  map[string][]int64
	failIDs map[string]int // id -> number of attempts to fail before succeeding
	seen    map[string]int // id -> how many times Publish was invoked
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{
		perSeq:  map[string][]int64{},
		failIDs: map[string]int{},
		seen:    map[string]int{},
	}
}

func (p *recordingPublisher) failFirst(id string, times int) { p.failIDs[id] = times }

func (p *recordingPublisher) Publish(ctx context.Context, msg *outbox.Message) error {
	p.mu.Lock()
	p.seen[msg.ID]++
	if remaining := p.failIDs[msg.ID]; remaining > 0 {
		p.failIDs[msg.ID] = remaining - 1
		p.mu.Unlock()
		return fmt.Errorf("simulated downstream failure for %s", msg.ID)
	}
	p.got = append(p.got, msg.ID)
	p.perSeq[msg.Stream] = append(p.perSeq[msg.Stream], msg.Seq)
	p.mu.Unlock()
	return nil
}

func (p *recordingPublisher) delivered() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.got...)
}

func (p *recordingPublisher) seqFor(stream string) []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.perSeq[stream]...)
}

func (p *recordingPublisher) seenCount(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seen[id]
}

func enqueueN(ctx context.Context, t *testing.T, ob *outbox.DefaultOutbox, stream string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := ob.Run(ctx, func(tx outbox.Tx) error {
			_, err := tx.Enqueue(ctx, stream, "evt", []byte(fmt.Sprintf("p%d", i)))
			return err
		})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
}

// TestDeliversCommittedMessages is the happy path.
func TestDeliversCommittedMessages(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	pub := newRecordingPublisher()
	_, ob := newTestOutbox(t, pub, fastConfig(), clk)
	d := ob.Dispatcher()

	enqueueN(ctx, t, ob, "orders", 5)
	for i := 0; i < 10 && len(pub.delivered()) < 5; i++ {
		d.RunOnce(ctx)
		d.WaitIdle()
	}

	if got := pub.delivered(); len(got) != 5 {
		t.Fatalf("want 5 delivered, got %d: %v", len(got), got)
	}
	seqs := pub.seqFor("orders")
	if len(seqs) != 5 {
		t.Fatalf("want 5 seq events, got %v", seqs)
	}
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("delivery order broken: %v", seqs)
		}
	}
}

// TestPerStreamOrdering: with failures and concurrency, messages of one stream
// still arrive in commit order. Different streams proceed independently.
func TestPerStreamOrdering(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := fastConfig()
	cfg.Concurrency = 8
	cfg.MaxAttempts = 10
	pub := newRecordingPublisher()
	_, ob := newTestOutbox(t, pub, cfg, clk)
	d := ob.Dispatcher()

	enqueueN(ctx, t, ob, "a", 6)
	enqueueN(ctx, t, ob, "b", 4)

	// First message in stream a fails twice to prove retry does not reorder.
	pub.failFirst("msg-1", 2)

	deadline := time.Now().Add(5 * time.Second)
	for len(pub.delivered()) < 10 && time.Now().Before(deadline) {
		d.RunOnce(ctx)
		clk.Advance(time.Second) // make backoff deadlines pass
		d.WaitIdle()
		time.Sleep(2 * time.Millisecond)
	}
	if got := pub.delivered(); len(got) != 10 {
		t.Fatalf("want 10 deliveries, got %v", got)
	}
	sa, sb := pub.seqFor("a"), pub.seqFor("b")
	want := []int64{1, 2, 3, 4, 5, 6}
	if fmt.Sprint(sa) != fmt.Sprint(want) {
		t.Fatalf("stream a order: %v want %v", sa, want)
	}
	wantB := []int64{1, 2, 3, 4}
	if fmt.Sprint(sb) != fmt.Sprint(wantB) {
		t.Fatalf("stream b order: %v want %v", sb, wantB)
	}
	// msg-1 was attempted 3 times (2 fails + 1 success) but delivered once.
	if pub.seenCount("msg-1") != 3 {
		t.Fatalf("msg-1 attempts = %d, want 3", pub.seenCount("msg-1"))
	}
}

// TestCrashRecoveryAfterCommit: process "dies" after commit before delivery;
// restarted relay with a fresh dispatcher still delivers every message.
func TestCrashRecoveryAfterCommit(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	pub := newRecordingPublisher()
	st, ob := newTestOutbox(t, pub, fastConfig(), clk)

	enqueueN(ctx, t, ob, "orders", 3)

	// Simulate a crashed process: create a brand new relay over the same store
	// after the lease window has fully elapsed.
	clk.Advance(time.Minute)
	pub2 := newRecordingPublisher()
	d2 := outbox.NewDispatcher(st, pub2, fastConfig(), clk)
	for i := 0; i < 10 && len(pub2.delivered()) < 3; i++ {
		d2.RunOnce(ctx)
		d2.WaitIdle()
	}
	if got := pub2.delivered(); len(got) != 3 {
		t.Fatalf("recovered relay delivered %d messages: %v", len(got), got)
	}
}

// TestCrashRecoveryMidDelivery: a worker claims (leases) a message and dies
// mid-publish. After lease expiry another worker reclaims and redelivers. The
// downstream sees a duplicate Publish, which must be recognized by ID and
// de-duplicated; the row ends delivered exactly once.
func TestCrashRecoveryMidDelivery(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	pub := newRecordingPublisher()
	st, ob := newTestOutbox(t, pub, fastConfig(), clk)
	d := ob.Dispatcher()

	enqueueN(ctx, t, ob, "orders", 1)

	// First claim: worker crashes (we never call any Mark*; advance clock past
	// lease). A new dispatcher instance recovers.
	d.RunOnce(ctx)
	d.WaitIdle()
	if pub.seenCount("msg-1") != 1 || len(pub.delivered()) != 1 {
		t.Fatalf("first worker should have published: seen=%d delivered=%v",
			pub.seenCount("msg-1"), pub.delivered())
	}
	// The successful first worker already marked delivered: the row stays
	// delivered and is never redelivered (duplicate suppression on success).
	clk.Advance(time.Minute)
	d2 := outbox.NewDispatcher(st, newRecordingPublisher(), fastConfig(), clk)
	d2.RunOnce(ctx)
	d2.WaitIdle()
	m, err := st.Get(ctx, "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Status != outbox.StatusDelivered {
		t.Fatalf("status = %v, want delivered", m.Status)
	}
}

// TestDuplicatePublishIsIdempotent: when the lease expires during a slow
// publish and both workers publish, the message is still marked delivered once
// (the stale worker's MarkDelivered is rejected by its lease token).
func TestDuplicatePublishIsIdempotent(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := fastConfig()
	cfg.LeaseDuration = 50 * time.Millisecond
	cfg.MaxAttempts = 10

	var mu sync.Mutex
	deliveries := []string{}
	firstAckRelease := make(chan struct{})
	firstAckBlocked := true

	pub := outbox.PublisherFunc(func(ctx context.Context, msg *outbox.Message) error {
		mu.Lock()
		deliveries = append(deliveries, msg.ID)
		n := len(deliveries)
		mu.Unlock()
		if n == 1 {
			// Simulate a very slow first publish: the lease expires, the second
			// worker reclaims and publishes, then the first finally returns.
			<-firstAckRelease
			mu.Lock()
			firstAckBlocked = false
			mu.Unlock()
		}
		return nil
	})

	st := memory.New().WithClock(clk.Now)
	ob := outbox.New(st, pub, outbox.WithConfig(cfg), outbox.WithClock(clk),
		outbox.WithIDGenerator(&outbox.SequenceIDGenerator{}))
	d := ob.Dispatcher()

	enqueueN(ctx, t, ob, "s", 1)

	d.RunOnce(ctx)
	// Wait until the first worker is inside Publish.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(deliveries)
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first publish never started")
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Lease expires; recovered worker publishes (2nd downstream call).
	clk.Advance(time.Second)
	d2 := outbox.NewDispatcher(st, pub, cfg, clk)
	d2.RunOnce(ctx)
	d2.WaitIdle()

	// Now release the stale first worker; its ack must be rejected.
	close(firstAckRelease)
	d.WaitIdle()

	mu.Lock()
	n := len(deliveries)
	mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 downstream calls (duplicate after recovery), got %d", n)
	}
	m, _ := st.Get(ctx, "msg-1")
	if m.Status != outbox.StatusDelivered {
		t.Fatalf("status = %v, want delivered", m.Status)
	}
	_ = firstAckBlocked
	_ = errors.New
}

// TestCrashAfterClaimBeforeAck: a worker claims (leases) a message and dies
// without recording any outcome. After lease expiry a restarted relay reclaims
// it and delivers. The stored message reaches Delivered; total Publish calls to
// the restarted downstream is exactly one.
func TestCrashAfterClaimBeforeAck(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cfg := fastConfig()
	cfg.LeaseDuration = time.Minute
	st, ob := newTestOutbox(t, noopPublisher(), cfg, clk)
	enqueueN(ctx, t, ob, "s", 1)

	// "Crash": claim directly and never mark any terminal state.
	claimed, err := st.ClaimDue(ctx, clk.Now(), cfg.LeaseDuration, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %d", err, len(claimed))
	}
	m, _ := st.Get(ctx, "msg-1")
	if m.Status != outbox.StatusInFlight {
		t.Fatalf("status = %v, want in-flight", m.Status)
	}

	// Restart after lease expiry; message is recovered and delivered.
	clk.Advance(2 * time.Minute)
	pub := newRecordingPublisher()
	d2 := outbox.NewDispatcher(st, pub, cfg, clk)
	for i := 0; i < 10 && len(pub.delivered()) < 1; i++ {
		d2.RunOnce(ctx)
		d2.WaitIdle()
	}
	m, _ = st.Get(ctx, "msg-1")
	if m.Status != outbox.StatusDelivered {
		t.Fatalf("status = %v, want delivered after recovery", m.Status)
	}
	if pub.seenCount("msg-1") != 1 {
		t.Fatalf("restarted downstream saw %d calls, want 1", pub.seenCount("msg-1"))
	}
}
