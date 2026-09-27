package outbox

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// Status is the lifecycle state of an outbox message.
type Status string

const (
	StatusPending   Status = "pending"
	StatusClaimed   Status = "claimed"
	StatusDelivered Status = "delivered"
	StatusDead      Status = "dead"
)

// Message is a single unit of work in the outbox.
// ID is the idempotency key downstream consumers must use to deduplicate.
type Message struct {
	ID      string
	Stream  string
	Seq     int64
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string

	Status      Status
	Attempts    int
	CreatedAt   time.Time
	DeliveredAt time.Time
	LastError   string

	// ClaimOwner is an opaque token set by the Store when the message is
	// claimed. It identifies one specific claim (not just one process), so a
	// worker whose lease expired and was re-claimed cannot finalize the
	// message. Dispatchers pass it back to MarkDelivered/Reschedule/MarkDead.
	ClaimOwner string
}

// NewID returns a random 128-bit message ID (UUIDv4-shaped).
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	var buf [36]byte
	hex.Encode(buf[0:8], b[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], b[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], b[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], b[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], b[10:16])
	return string(buf[:])
}

func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), NewID()[:8])
}

// StreamProgress reports per-stream delivery progress.
type StreamProgress struct {
	Stream           string
	Pending          int
	Claimed          int
	Delivered        int
	Dead             int
	NextSeq          int64
	LastDeliveredSeq int64
}

// Stats reports store-wide message counts by status.
type Stats struct {
	Pending   int
	Claimed   int
	Delivered int
	Dead      int
}
