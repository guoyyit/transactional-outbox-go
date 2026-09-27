package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// schema is created idempotently at open time.
const schema = `
CREATE TABLE IF NOT EXISTS outbox_messages (
    id            TEXT PRIMARY KEY,
    stream        TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    topic         TEXT NOT NULL,
    msg_key       TEXT NOT NULL DEFAULT '',
    payload       BLOB,
    headers       TEXT NOT NULL DEFAULT '{}',
    status        TEXT NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at INTEGER NOT NULL DEFAULT 0,
    created_at    INTEGER NOT NULL,
    delivered_at  INTEGER,
    lease_owner   TEXT NOT NULL DEFAULT '',
    lease_until   INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT NOT NULL DEFAULT '',
    UNIQUE(stream, seq)
);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox_messages(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_outbox_stream ON outbox_messages(stream, seq);
CREATE INDEX IF NOT EXISTS idx_outbox_cleanup ON outbox_messages(status, delivered_at);
`

// messageColumns is the canonical SELECT column list for scanMessage.
const messageColumns = `id, stream, seq, topic, msg_key, payload, headers, status, attempts,
	next_retry_at, created_at, delivered_at, lease_owner, lease_until, last_error`

// unresolvedStatuses block later messages of the same stream (head-of-line).
const unresolvedStatuses = `('pending','claimed','dead')`

// SQLiteOption customizes OpenSQLite.
type SQLiteOption func(*SQLiteStore)

// WithMaxPending bounds the number of undelivered (pending+claimed)
// messages; Tx.Enqueue fails with ErrOutboxFull once reached, giving
// business writers backpressure so the outbox cannot grow without bound.
func WithMaxPending(n int) SQLiteOption {
	return func(s *SQLiteStore) { s.maxPending = n }
}

// SQLiteStore is a Store backed by a SQLite database file.
//
// A single process should own writes to the database file; per-stream
// sequence assignment and the pending cap are serialized by an in-process
// write mutex, and the UNIQUE(stream, seq) constraint protects against
// cross-process races.
type SQLiteStore struct {
	db         *sql.DB
	maxPending int
	wmu        sync.Mutex // serializes RunInTx writers (SQLite is single-writer anyway)
}

// OpenSQLite opens (creating if needed) a SQLite-backed store. path may be a
// file path, ":memory:", or a full "file:" DSN.
func OpenSQLite(path string, opts ...SQLiteOption) (*SQLiteStore, error) {
	dsn := path
	switch {
	case path == ":memory:":
		dsn = "file::memory:?cache=shared&_pragma=busy_timeout(5000)"
	case !strings.HasPrefix(path, "file:"):
		dsn = "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("outbox: open sqlite: %w", err)
	}
	if path == ":memory:" {
		// A shared-cache in-memory database vanishes when its last
		// connection closes; pin the pool to a single connection.
		db.SetMaxOpenConns(1)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("outbox: ping sqlite: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("outbox: init schema: %w", err)
	}
	s := &SQLiteStore{db: db}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// sqliteTx implements Tx over *sql.Tx.
type sqliteTx struct {
	tx    *sql.Tx
	store *SQLiteStore
}

// Raw returns the underlying *sql.Tx for business writes.
func (t *sqliteTx) Raw() any { return t.tx }

// Enqueue stages msg in the outbox, assigning its per-stream sequence number.
func (t *sqliteTx) Enqueue(ctx context.Context, msg *Message) error {
	if msg.Stream == "" {
		return errors.New("outbox: message stream is required")
	}
	if msg.Topic == "" {
		return errors.New("outbox: message topic is required")
	}
	if t.store.maxPending > 0 {
		var n int
		err := t.tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM outbox_messages WHERE status IN ('pending','claimed')`).Scan(&n)
		if err != nil {
			return err
		}
		if n >= t.store.maxPending {
			return ErrOutboxFull
		}
	}
	if msg.ID == "" {
		msg.ID = NewID()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	var seq int64
	err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq),0)+1 FROM outbox_messages WHERE stream = ?`, msg.Stream).Scan(&seq)
	if err != nil {
		return fmt.Errorf("outbox: assign seq: %w", err)
	}
	headers := []byte("{}")
	if len(msg.Headers) > 0 {
		if headers, err = json.Marshal(msg.Headers); err != nil {
			return fmt.Errorf("outbox: marshal headers: %w", err)
		}
	}
	_, err = t.tx.ExecContext(ctx, `
INSERT INTO outbox_messages
    (id, stream, seq, topic, msg_key, payload, headers, status, attempts,
     next_retry_at, created_at, delivered_at, lease_owner, lease_until, last_error)
VALUES (?,?,?,?,?,?,?,?,0,?,?,NULL,'',0,'')`,
		msg.ID, msg.Stream, seq, msg.Topic, msg.Key, msg.Payload, headers,
		string(StatusPending), msg.CreatedAt.UnixNano(), msg.CreatedAt.UnixNano())
	if err != nil {
		return fmt.Errorf("outbox: insert message: %w", err)
	}
	msg.Seq = seq
	msg.Status = StatusPending
	return nil
}

// RunInTx executes fn inside one SQLite transaction. The write mutex
// serializes concurrent writers so per-stream sequence assignment and the
// pending cap are race-free within the process.
func (s *SQLiteStore) RunInTx(ctx context.Context, fn func(Tx) error) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("outbox: begin tx: %w", err)
	}
	if err := fn(&sqliteTx{tx: tx, store: s}); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("outbox: commit tx: %w", err)
	}
	return nil
}

// dueHeads selects IDs of due, deliverable stream-head messages.
const dueHeadsQuery = `
SELECT m.id FROM outbox_messages m
WHERE m.status = 'pending' AND m.next_retry_at <= ?
  AND NOT EXISTS (
      SELECT 1 FROM outbox_messages p
      WHERE p.stream = m.stream AND p.seq < m.seq
        AND p.status IN ` + unresolvedStatuses + `
  )
ORDER BY m.stream, m.seq
LIMIT ?`

// claimOne atomically transitions one pending message to claimed.
const claimStmt = `
UPDATE outbox_messages
SET status = 'claimed', lease_owner = ?, lease_until = ?, attempts = attempts + 1
WHERE id = ? AND status = 'pending'
RETURNING ` + messageColumns

func (s *SQLiteStore) ClaimDue(ctx context.Context, now time.Time, limit int, owner string, lease time.Duration) ([]*Message, error) {
	rows, err := s.db.QueryContext(ctx, dueHeadsQuery, now.UnixNano(), limit)
	if err != nil {
		return nil, fmt.Errorf("outbox: select due: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	leaseUntil := now.Add(lease).UnixNano()
	claimed := make([]*Message, 0, len(ids))
	for _, id := range ids {
		msg, err := s.claimByID(ctx, id, owner, leaseUntil)
		if errors.Is(err, sql.ErrNoRows) {
			continue // lost the race; another owner took it
		}
		if err != nil {
			return claimed, err
		}
		claimed = append(claimed, msg)
	}
	return claimed, nil
}

func (s *SQLiteStore) claimByID(ctx context.Context, id, owner string, leaseUntil int64) (*Message, error) {
	row := s.db.QueryRowContext(ctx, claimStmt, owner, leaseUntil, id)
	return scanMessage(row)
}

// ClaimStreamHead claims the next deliverable message of one stream.
func (s *SQLiteStore) ClaimStreamHead(ctx context.Context, stream, owner string, lease time.Duration, now time.Time) (*Message, error) {
	row := s.db.QueryRowContext(ctx, `
UPDATE outbox_messages
SET status = 'claimed', lease_owner = ?, lease_until = ?, attempts = attempts + 1
WHERE id = (
    SELECT m.id FROM outbox_messages m
    WHERE m.stream = ? AND m.status = 'pending' AND m.next_retry_at <= ?
      AND NOT EXISTS (
          SELECT 1 FROM outbox_messages p
          WHERE p.stream = m.stream AND p.seq < m.seq
            AND p.status IN `+unresolvedStatuses+`
      )
    ORDER BY m.seq LIMIT 1
) AND status = 'pending'
RETURNING `+messageColumns,
		owner, now.Add(lease).UnixNano(), stream, now.UnixNano())
	msg, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("outbox: claim stream head: %w", err)
	}
	return msg, nil
}

func (s *SQLiteStore) MarkDelivered(ctx context.Context, id, owner string, at time.Time) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE outbox_messages
SET status = 'delivered', delivered_at = ?, lease_owner = '', lease_until = 0, last_error = ''
WHERE id = ? AND status = 'claimed' AND lease_owner = ?`,
		at.UnixNano(), id, owner)
	if err != nil {
		return false, fmt.Errorf("outbox: mark delivered: %w", err)
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func (s *SQLiteStore) Reschedule(ctx context.Context, id, owner string, nextRetryAt time.Time, lastErr string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE outbox_messages
SET status = 'pending', next_retry_at = ?, last_error = ?, lease_owner = '', lease_until = 0
WHERE id = ? AND status = 'claimed' AND lease_owner = ?`,
		nextRetryAt.UnixNano(), truncateErr(lastErr), id, owner)
	if err != nil {
		return fmt.Errorf("outbox: reschedule: %w", err)
	}
	return nil
}

func (s *SQLiteStore) MarkDead(ctx context.Context, id, owner string, lastErr string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE outbox_messages
SET status = 'dead', last_error = ?, lease_owner = '', lease_until = 0
WHERE id = ? AND status = 'claimed' AND lease_owner = ?`,
		truncateErr(lastErr), id, owner)
	if err != nil {
		return fmt.Errorf("outbox: mark dead: %w", err)
	}
	return nil
}

func (s *SQLiteStore) ReclaimExpired(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE outbox_messages
SET status = 'pending', lease_owner = '', lease_until = 0
WHERE status = 'claimed' AND lease_until < ?`, now.UnixNano())
	if err != nil {
		return 0, fmt.Errorf("outbox: reclaim expired: %w", err)
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) ResetClaimed(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE outbox_messages
SET status = 'pending', lease_owner = '', lease_until = 0
WHERE status = 'claimed'`)
	if err != nil {
		return 0, fmt.Errorf("outbox: reset claimed: %w", err)
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) DeleteDelivered(ctx context.Context, before time.Time, limit int) (int, error) {
	res, err := s.db.ExecContext(ctx, `
DELETE FROM outbox_messages
WHERE id IN (
    SELECT id FROM outbox_messages
    WHERE status = 'delivered' AND delivered_at IS NOT NULL AND delivered_at < ?
    LIMIT ?)`, before.UnixNano(), limit)
	if err != nil {
		return 0, fmt.Errorf("outbox: delete delivered: %w", err)
	}
	n, err := res.RowsAffected()
	return int(n), err
}

func (s *SQLiteStore) PendingCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbox_messages WHERE status IN ('pending','claimed')`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("outbox: pending count: %w", err)
	}
	return n, nil
}

func (s *SQLiteStore) DeadLetters(ctx context.Context, stream string, limit, offset int) ([]*Message, error) {
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT ` + messageColumns + ` FROM outbox_messages WHERE status = 'dead'`
	args := []any{}
	if stream != "" {
		q += ` AND stream = ?`
		args = append(args, stream)
	}
	q += ` ORDER BY created_at LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("outbox: dead letters: %w", err)
	}
	return scanMessages(rows)
}

func (s *SQLiteStore) ReplayDead(ctx context.Context, id string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `
UPDATE outbox_messages
SET status = 'pending', attempts = 0, next_retry_at = ?, last_error = '',
    lease_owner = '', lease_until = 0
WHERE id = ? AND status = 'dead'`, now.UnixNano(), id)
	if err != nil {
		return fmt.Errorf("outbox: replay dead: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	var status string
	err = s.db.QueryRowContext(ctx, `SELECT status FROM outbox_messages WHERE id = ?`, id).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: id=%s status=%s", ErrNotDead, id, status)
}

func (s *SQLiteStore) StreamProgress(ctx context.Context, stream string) (StreamProgress, error) {
	p := StreamProgress{Stream: stream}
	err := s.db.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(status = 'pending'), 0),
  COALESCE(SUM(status = 'claimed'), 0),
  COALESCE(SUM(status = 'delivered'), 0),
  COALESCE(SUM(status = 'dead'), 0),
  COALESCE(MAX(seq), 0),
  COALESCE(MAX(CASE WHEN status = 'delivered' THEN seq END), 0)
FROM outbox_messages WHERE stream = ?`, stream).
		Scan(&p.Pending, &p.Claimed, &p.Delivered, &p.Dead, &p.NextSeq, &p.LastDeliveredSeq)
	if err != nil {
		return p, fmt.Errorf("outbox: stream progress: %w", err)
	}
	p.NextSeq++
	return p, nil
}

func (s *SQLiteStore) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.db.QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(status = 'pending'), 0),
  COALESCE(SUM(status = 'claimed'), 0),
  COALESCE(SUM(status = 'delivered'), 0),
  COALESCE(SUM(status = 'dead'), 0)
FROM outbox_messages`).
		Scan(&st.Pending, &st.Claimed, &st.Delivered, &st.Dead)
	if err != nil {
		return st, fmt.Errorf("outbox: stats: %w", err)
	}
	return st, nil
}

// Close closes the underlying database.
func (s *SQLiteStore) Close() error { return s.db.Close() }

// scanner abstracts *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanMessage(sc scanner) (*Message, error) {
	var (
		m          Message
		status     string
		headers    string
		nextRetry  int64
		createdAt  int64
		delivered  sql.NullInt64
		leaseOwner string
		leaseUntil int64
	)
	err := sc.Scan(&m.ID, &m.Stream, &m.Seq, &m.Topic, &m.Key, &m.Payload, &headers,
		&status, &m.Attempts, &nextRetry, &createdAt, &delivered, &leaseOwner, &leaseUntil, &m.LastError)
	if err != nil {
		return nil, err
	}
	m.Status = Status(status)
	m.ClaimOwner = leaseOwner
	m.CreatedAt = time.Unix(0, createdAt)
	if delivered.Valid {
		m.DeliveredAt = time.Unix(0, delivered.Int64)
	}
	if headers != "" && headers != "{}" {
		if err := json.Unmarshal([]byte(headers), &m.Headers); err != nil {
			return nil, fmt.Errorf("outbox: decode headers of %s: %w", m.ID, err)
		}
	}
	return &m, nil
}

func scanMessages(rows *sql.Rows) ([]*Message, error) {
	defer rows.Close()
	var out []*Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// truncateErr keeps stored error strings bounded.
func truncateErr(s string) string {
	const max = 1024
	if len(s) > max {
		return s[:max]
	}
	return s
}
