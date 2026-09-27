package outbox

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// --- helpers ---------------------------------------------------------------

func tempStore(t *testing.T, opts ...SQLiteOption) *SQLiteStore {
	t.Helper()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "outbox.db"), opts...)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// recorder is a Publisher capturing delivered messages and publish attempts.
type recorder struct {
	mu       sync.Mutex
	msgs     []*Message // successful publishes, in order
	attempts map[string]int
	times    map[string][]time.Time
	fail     func(msg *Message) error // optional failure hook
}

func newRecorder() *recorder {
	return &recorder{attempts: map[string]int{}, times: map[string][]time.Time{}}
}

func (r *recorder) Publish(ctx context.Context, msg *Message) error {
	r.mu.Lock()
	r.attempts[msg.ID]++
	r.times[msg.ID] = append(r.times[msg.ID], time.Now())
	hook := r.fail
	r.mu.Unlock()
	if hook != nil {
		if err := hook(msg); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, msg)
	r.mu.Unlock()
	return nil
}

func (r *recorder) delivered() []*Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*Message(nil), r.msgs...)
}

func (r *recorder) attemptsFor(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[id]
}

func (r *recorder) attemptTimes(id string) []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.times[id]...)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func fastConfig() Config {
	return Config{
		PollInterval:    10 * time.Millisecond,
		ReclaimInterval: 50 * time.Millisecond,
		CleanupInterval: 20 * time.Millisecond,
		BaseBackoff:     20 * time.Millisecond,
		MaxBackoff:      100 * time.Millisecond,
		LeaseDuration:   2 * time.Second,
		PublishTimeout:  time.Second,
	}
}

// --- tests -----------------------------------------------------------------

// TestWriteCommitRollback proves business writes and outbox inserts commit
// and roll back atomically.
func TestWriteCommitRollback(t *testing.T) {
	store := tempStore(t)
	ctx := context.Background()

	// Business table lives in the same database file.
	if _, err := store.db.Exec(`CREATE TABLE biz (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	ob, err := New(store, rec, fastConfig())
	if err != nil {
		t.Fatal(err)
	}

	// Rollback path: enqueue then fail the transaction.
	errBoom := errors.New("business failure")
	err = ob.Write(ctx, func(tx Tx) error {
		if _, err := tx.Raw().(*sql.Tx).ExecContext(ctx, `INSERT INTO biz VALUES ('o1')`); err != nil {
			t.Fatalf("biz insert: %v", err)
		}
		if err := tx.Enqueue(ctx, &Message{Stream: "orders", Topic: "order.created", Payload: []byte("o1")}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("want errBoom, got %v", err)
	}
	var bizRows, outboxRows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM biz`).Scan(&bizRows); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM outbox_messages`).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if bizRows != 0 || outboxRows != 0 {
		t.Fatalf("rollback leaked state: biz=%d outbox=%d", bizRows, outboxRows)
	}

	// Commit path: both writes persist, ID and Seq are assigned.
	var msgID string
	var seq int64
	err = ob.Write(ctx, func(tx Tx) error {
		if _, err := tx.Raw().(*sql.Tx).ExecContext(ctx, `INSERT INTO biz VALUES ('o2')`); err != nil {
			return err
		}
		m := &Message{Stream: "orders", Topic: "order.created", Payload: []byte("o2")}
		if err := tx.Enqueue(ctx, m); err != nil {
			return err
		}
		msgID, seq = m.ID, m.Seq
		return nil
	})
	if err != nil {
		t.Fatalf("commit write: %v", err)
	}
	if msgID == "" || seq != 1 {
		t.Fatalf("expected assigned ID and seq=1, got id=%q seq=%d", msgID, seq)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM biz`).Scan(&bizRows); err != nil {
		t.Fatal(err)
	}
	st, err := ob.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if bizRows != 1 || st.Pending != 1 {
		t.Fatalf("commit missing state: biz=%d pending=%d", bizRows, st.Pending)
	}

	// The committed message is deliverable after Start.
	if err := ob.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer ob.Stop(context.Background())
	waitFor(t, "delivery", func() bool { return len(rec.delivered()) == 1 })
	if got := rec.delivered()[0].ID; got != msgID {
		t.Fatalf("delivered wrong message: %q", got)
	}
}
