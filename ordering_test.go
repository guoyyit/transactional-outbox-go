package outbox

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestOrderingPerStream verifies messages of one stream are delivered in
// commit order, across many concurrently delivered streams.
func TestOrderingPerStream(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()
	rec := newRecorder()
	ob, err := New(store, rec, fastConfig())
	if err != nil {
		t.Fatal(err)
	}

	const streams = 8
	const perStream = 25
	for s := 0; s < streams; s++ {
		stream := fmt.Sprintf("stream-%d", s)
		for i := 0; i < perStream; i++ {
			i := i
			if err := ob.Write(ctx, func(tx Tx) error {
				return tx.Enqueue(ctx, &Message{Stream: stream, Topic: "evt", Payload: []byte(fmt.Sprintf("%s/%d", stream, i))})
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	if err := ob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ob.Stop(context.Background())

	waitFor(t, "all delivered", func() bool { return len(rec.delivered()) == streams*perStream })

	// Per stream, delivered sequence numbers must be exactly 1..N in order.
	seen := map[string][]int64{}
	for _, m := range rec.delivered() {
		seen[m.Stream] = append(seen[m.Stream], m.Seq)
	}
	for s := 0; s < streams; s++ {
		stream := fmt.Sprintf("stream-%d", s)
		got := seen[stream]
		if len(got) != perStream {
			t.Fatalf("%s: delivered %d messages, want %d", stream, len(got), perStream)
		}
		for i, seq := range got {
			if seq != int64(i+1) {
				t.Fatalf("%s: position %d has seq %d, want %d (order violated)", stream, i, seq, i+1)
			}
		}
	}

	// Per-stream progress is traceable.
	p, err := ob.StreamProgress(ctx, "stream-3")
	if err != nil {
		t.Fatal(err)
	}
	if p.Delivered != perStream || p.Pending != 0 || p.NextSeq != perStream+1 || p.LastDeliveredSeq != perStream {
		t.Fatalf("unexpected progress: %+v", p)
	}
}

// TestConcurrentStreamsIndependent verifies a blocked stream does not stall
// other streams, and concurrent writers do not block each other.
func TestConcurrentStreamsIndependent(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()
	rec := newRecorder()

	// Stream "stuck" always fails; everything else succeeds.
	rec.fail = func(msg *Message) error {
		if msg.Stream == "stuck" {
			return fmt.Errorf("downstream rejects %s", msg.Stream)
		}
		return nil
	}

	cfg := fastConfig()
	cfg.MaxAttempts = 3
	ob, err := New(store, rec, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent writers on independent streams.
	var wg sync.WaitGroup
	for s := 0; s < 6; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			stream := fmt.Sprintf("ok-%d", s)
			for i := 0; i < 10; i++ {
				if err := ob.Write(ctx, func(tx Tx) error {
					return tx.Enqueue(ctx, &Message{Stream: stream, Topic: "evt"})
				}); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(s)
	}
	// The poisoned stream.
	if err := ob.Write(ctx, func(tx Tx) error {
		return tx.Enqueue(ctx, &Message{Stream: "stuck", Topic: "evt"})
	}); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if err := ob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ob.Stop(context.Background())

	// Healthy streams fully deliver despite the stuck stream.
	waitFor(t, "healthy streams delivered", func() bool { return len(rec.delivered()) == 60 })

	// The stuck stream exhausts attempts and lands in the dead-letter set.
	waitFor(t, "stuck message dead", func() bool {
		p, err := ob.StreamProgress(ctx, "stuck")
		return err == nil && p.Dead == 1
	})
	dead, err := ob.DeadLetters(ctx, "stuck", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Attempts != 3 {
		t.Fatalf("dead letters: %+v", dead)
	}
}
