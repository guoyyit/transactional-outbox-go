// Package memory is a transactional in-process implementation of outbox.Store.
// It is intended for tests, examples and single-process deployments.
package memory

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/guoyyit/transactional-outbox-go/outbox"
)

// ErrConflict indicates an optimistic-concurrency collision between two
// concurrent business transactions; Transaction retries automatically.
var ErrConflict = errors.New("memory: transaction conflict")

// Store is an in-memory transactional Store.
type Store struct {
	mu sync.Mutex

	kv        map[string][]byte
	messages  map[string]*outbox.Message // id -> msg
	order     []string                   // message IDs in commit order
	streamSeq map[string]int64
	keyGen    map[string]uint64 // last transaction generation that touched a key
	txGen     uint64            // incremented per committed business transaction

	now func() time.Time

	// maxMessages, when > 0, is a hard store-level bound on active rows. The
	// outbox normally enforces Config.MaxOutboxSize on admission; this is a
	// defense-in-depth cap for direct store users.
	maxMessages int
}

// New creates an empty memory store.
func New() *Store {
	return &Store{
		kv:        map[string][]byte{},
		messages:  map[string]*outbox.Message{},
		streamSeq: map[string]int64{},
		keyGen:    map[string]uint64{},
		now:       time.Now,
	}
}

// WithMaxActive caps the number of active (non-delivered) messages.
func (s *Store) WithMaxActive(n int) *Store {
	s.mu.Lock()
	s.maxMessages = n
	s.mu.Unlock()
	return s
}

// WithClock injects a clock for deterministic tests.
func (s *Store) WithClock(now func() time.Time) *Store {
	s.mu.Lock()
	s.now = now
	s.mu.Unlock()
	return s
}

const maxTxRetries = 16

// Transaction executes fn with write-buffering and commits atomically. On an
// optimistic-key conflict the whole function is retried; on fn error (or
// admission/duplicate errors) all buffered writes are discarded.
func (s *Store) Transaction(ctx context.Context, fn func(tx outbox.StoreTx) error) error {
	var lastErr error
	for attempt := 0; attempt < maxTxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		baseGen := s.txGen
		s.mu.Unlock()

		t := &tx{store: s, baseGen: baseGen, kvOps: map[string]kvOp{}}
		if err := fn(t); err != nil {
			return err // rollback: buffered writes are simply dropped
		}

		s.mu.Lock()
		if err := s.commitLocked(t); err != nil {
			lastErr = err
			s.mu.Unlock()
			if errors.Is(err, ErrConflict) {
				continue
			}
			return err
		}
		s.mu.Unlock()
		return nil
	}
	return lastErr
}

type kvOp struct {
	delete bool
	value  []byte
}

type tx struct {
	store   *Store
	baseGen uint64
	kvOps   map[string]kvOp
	pending []*outbox.Message
}

func (t *tx) PutKV(_ context.Context, key string, value []byte) error {
	t.kvOps[key] = kvOp{value: append([]byte(nil), value...)}
	return nil
}

func (t *tx) DeleteKV(_ context.Context, key string) error {
	t.kvOps[key] = kvOp{delete: true}
	return nil
}

func (t *tx) InsertMessage(_ context.Context, msg *outbox.Message) error {
	if msg.ID == "" {
		return errors.New("memory: message id is empty")
	}
	if msg.Stream == "" {
		return errors.New("memory: stream is empty")
	}
	t.pending = append(t.pending, msg)
	return nil
}

// ActiveCount counts committed active messages plus this tx's staged messages.
func (t *tx) ActiveCount(_ context.Context) (int, error) {
	s := t.store
	s.mu.Lock()
	n := 0
	for _, m := range s.messages {
		if m.Status != outbox.StatusDelivered {
			n++
		}
	}
	s.mu.Unlock()
	return n + len(t.pending), nil
}

func (s *Store) commitLocked(t *tx) error {
	// Optimistic conflict detection on touched business keys.
	for k := range t.kvOps {
		if s.keyGen[k] > t.baseGen {
			return ErrConflict
		}
	}
	// Re-check the backlog bound atomically with commit.
	active := 0
	for _, m := range s.messages {
		if m.Status != outbox.StatusDelivered {
			active++
		}
	}
	// Duplicate IDs are rejected up front.
	for _, p := range t.pending {
		if _, exists := s.messages[p.ID]; exists {
			return errors.New("memory: duplicate message id " + p.ID)
		}
	}
	if s.maxMessages > 0 && active+len(t.pending) > s.maxMessages {
		return outbox.ErrOutboxFull
	}

	// Assign per-stream monotonic Seq in staged (Enqueue call) order. Seq is
	// allocated at commit time, which is the only moment the global order
	// becomes observable.
	for _, p := range t.pending {
		s.streamSeq[p.Stream]++
		p.Seq = s.streamSeq[p.Stream]
	}
	// Apply business writes.
	for k, op := range t.kvOps {
		if op.delete {
			delete(s.kv, k)
		} else {
			s.kv[k] = append([]byte(nil), op.value...)
		}
		s.keyGen[k] = s.txGen + 1
	}
	// Apply outbox messages.
	for _, p := range t.pending {
		cp := *p
		s.messages[p.ID] = &cp
		s.order = append(s.order, p.ID)
	}
	s.txGen++
	return nil
}

// ClaimDue leases due messages, at most one per stream, strictly in Seq order.
// It returns clones so callers can never mutate committed state directly.
func (s *Store) ClaimDue(_ context.Context, now time.Time, lease time.Duration, batch int) ([]*outbox.Message, error) {
	if batch <= 0 {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Group active messages by stream and sort by Seq.
	heads := map[string][]*outbox.Message{}
	for _, m := range s.messages {
		if m.Status == outbox.StatusDelivered {
			continue
		}
		heads[m.Stream] = append(heads[m.Stream], m)
	}
	streams := make([]string, 0, len(heads))
	for st := range heads {
		sort.Slice(heads[st], func(i, j int) bool { return heads[st][i].Seq < heads[st][j].Seq })
		streams = append(streams, st)
	}
	sort.Strings(streams)

	claimed := make([]*outbox.Message, 0, batch)
	claimedStreams := map[string]bool{}
	// Round-robin across streams: at most ONE message per stream per scan, so
	// each stream's head is the only message of that stream ever in flight.
	// Combined with releasing the head only after a terminal outcome
	// (delivered/retry/dead), this guarantees per-stream commit-order delivery.
	// Rotating across streams keeps independent business writers from starving.
	for _, st := range streams {
		if len(claimed) >= batch {
			break
		}
		if claimedStreams[st] {
			continue
		}
		row := heads[st]
		if len(row) == 0 {
			continue
		}
		head := row[0]
		switch head.Status {
		case outbox.StatusDead:
			continue // blocked until DLQ replay
		case outbox.StatusInFlight:
			if !head.LeaseUntil.After(now) {
				// Crashed worker: recover the lease.
				head.Status = outbox.StatusPending
				head.LeaseUntil = time.Time{}
			} else {
				continue // previous message of this stream is still being worked on
			}
		}
		if head.Status == outbox.StatusPending && head.AvailableAt.After(now) {
			continue // backing off; blocks the whole stream in order
		}
		head.Status = outbox.StatusInFlight
		head.Attempt++
		head.LeaseUntil = now.Add(lease)
		head.UpdatedAt = now
		cp := *head
		claimed = append(claimed, &cp)
		claimedStreams[st] = true
	}
	return claimed, nil
}

// ErrLeaseLost is the memory store's lease error; it is the same sentinel as
// outbox.ErrLeaseLost so callers can use errors.Is against either.
var ErrLeaseLost = outbox.ErrLeaseLost

func (s *Store) checkLease(m *outbox.Message, leaseExpiry time.Time) bool {
	return m.Status == outbox.StatusInFlight && m.LeaseUntil.Equal(leaseExpiry)
}

// MarkDelivered marks a leased message delivered iff the lease token matches.
func (s *Store) MarkDelivered(_ context.Context, id string, leaseExpiry, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.messages[id]
	if m == nil || !s.checkLease(m, leaseExpiry) {
		return ErrLeaseLost
	}
	m.Status = outbox.StatusDelivered
	m.LeaseUntil = time.Time{}
	m.UpdatedAt = now
	return nil
}

// MarkRetry releases a leased message back to pending with a backoff deadline.
func (s *Store) MarkRetry(_ context.Context, id string, leaseExpiry, now, availableAt time.Time, lastErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.messages[id]
	if m == nil || !s.checkLease(m, leaseExpiry) {
		return ErrLeaseLost
	}
	m.Status = outbox.StatusPending
	m.LeaseUntil = time.Time{}
	m.AvailableAt = availableAt
	m.LastError = lastErr
	m.UpdatedAt = now
	return nil
}

// MarkDead moves a leased message into the independent dead-letter set.
func (s *Store) MarkDead(_ context.Context, id string, leaseExpiry, now time.Time, lastErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.messages[id]
	if m == nil || !s.checkLease(m, leaseExpiry) {
		return ErrLeaseLost
	}
	m.Status = outbox.StatusDead
	m.LeaseUntil = time.Time{}
	m.AvailableAt = now
	m.LastError = lastErr
	m.UpdatedAt = now
	return nil
}

// Replay requeues a dead message and resets its attempt budget. Because
// business side effects were committed in the original transaction, replay
// only reschedules the outbound message — nothing is re-executed downstream
// except the (idempotent) publish.
func (s *Store) Replay(_ context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.messages[id]
	if m == nil {
		return errors.New("memory: message not found: " + id)
	}
	if m.Status != outbox.StatusDead {
		return errors.New("memory: message is not dead: " + id)
	}
	m.Status = outbox.StatusPending
	m.Attempt = 0
	m.LastError = ""
	m.AvailableAt = now
	m.LeaseUntil = time.Time{}
	m.UpdatedAt = now
	return nil
}

// ReplayAll requeues all dead messages for a stream (empty = every stream).
func (s *Store) ReplayAll(_ context.Context, stream string, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, id := range s.order {
		m := s.messages[id]
		if m.Status != outbox.StatusDead || (stream != "" && m.Stream != stream) {
			continue
		}
		m.Status = outbox.StatusPending
		m.Attempt = 0
		m.LastError = ""
		m.AvailableAt = now
		m.LeaseUntil = time.Time{}
		m.UpdatedAt = now
		n++
	}
	return n, nil
}

// DeleteDelivered removes delivered rows whose UpdatedAt is before the cutoff,
// strictly limited to StatusDelivered — pending/in-flight/dead rows are never
// touched, so concurrent delivery cannot create dangling or duplicate sends.
func (s *Store) DeleteDelivered(_ context.Context, before time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	kept := s.order[:0]
	for _, id := range s.order {
		m := s.messages[id]
		if n < limit && m.Status == outbox.StatusDelivered && m.UpdatedAt.Before(before) {
			delete(s.messages, id)
			n++
			continue
		}
		kept = append(kept, id)
	}
	s.order = append([]string(nil), kept...)
	return n, nil
}

// ListDead returns dead messages newest first (by UpdatedAt, then Seq).
func (s *Store) ListDead(_ context.Context, stream string, limit, offset int) ([]*outbox.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rows []*outbox.Message
	for _, m := range s.messages {
		if m.Status == outbox.StatusDead && (stream == "" || m.Stream == stream) {
			cp := *m
			rows = append(rows, &cp)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].UpdatedAt.Equal(rows[j].UpdatedAt) {
			return rows[i].UpdatedAt.After(rows[j].UpdatedAt)
		}
		return rows[i].Seq > rows[j].Seq
	})
	if offset >= len(rows) {
		return nil, nil
	}
	rows = rows[offset:]
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// Get returns a clone of a message.
func (s *Store) Get(_ context.Context, id string) (*outbox.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.messages[id]
	if m == nil {
		return nil, nil
	}
	cp := *m
	return &cp, nil
}

// StreamStats reports per-stream progress counters.
func (s *Store) StreamStats(_ context.Context) ([]outbox.StreamStat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := map[string]*outbox.StreamStat{}
	for _, m := range s.messages {
		st := stats[m.Stream]
		if st == nil {
			st = &outbox.StreamStat{Stream: m.Stream}
			stats[m.Stream] = st
		}
		switch m.Status {
		case outbox.StatusPending:
			st.Pending++
		case outbox.StatusInFlight:
			st.InFlight++
		case outbox.StatusDelivered:
			st.Delivered++
		case outbox.StatusDead:
			st.Dead++
		}
		if m.Seq > st.LastSeq {
			st.LastSeq = m.Seq
		}
	}
	out := make([]outbox.StreamStat, 0, len(stats))
	for _, st := range stats {
		out = append(out, *st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Stream < out[j].Stream })
	return out, nil
}

// Size returns the count of active (non-delivered) messages.
func (s *Store) Size(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.messages {
		if m.Status != outbox.StatusDelivered {
			n++
		}
	}
	return n, nil
}

// KVGet reads committed business data (helper for tests/demos).
func (s *Store) KVGet(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}
