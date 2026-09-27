package outbox

import (
	"context"
	"errors"
)

// Option configures a new Outbox instance.
type Option func(*options)

type options struct {
	config Config
	idGen  IDGenerator
	clock  Clock
}

// WithConfig sets the full configuration.
func WithConfig(c Config) Option {
	return func(o *options) { o.config = c }
}

// WithIDGenerator overrides the default ID generator.
func WithIDGenerator(g IDGenerator) Option {
	return func(o *options) { o.idGen = g }
}

// WithClock injects a clock (mainly for tests).
func WithClock(c Clock) Option {
	return func(o *options) { o.clock = c }
}

// ErrOutboxFull is returned when MaxOutboxSize would be exceeded.
var ErrOutboxFull = errors.New("outbox: backlog limit exceeded")

// DefaultOutbox wires a store, publisher and config into a runnable outbox:
// an Outbox writer plus dispatcher, DLQ and cleaner subsystems.
type DefaultOutbox struct {
	store      Store
	publisher  Publisher
	cfg        Config
	idGen      IDGenerator
	clock      Clock
	dispatcher *Dispatcher
	dlq        *DLQ
	cleaner    *Cleaner
}

// New builds a DefaultOutbox.
func New(store Store, publisher Publisher, opts ...Option) *DefaultOutbox {
	o := options{config: DefaultConfig()}
	for _, opt := range opts {
		opt(&o)
	}
	cfg := o.config.withDefaults()
	clock := o.clock
	if clock == nil {
		clock = SystemClock{}
	}
	idGen := o.idGen
	if idGen == nil {
		idGen = RandomIDGenerator{}
	}
	d := &DefaultOutbox{
		store:     store,
		publisher: publisher,
		cfg:       cfg,
		idGen:     idGen,
		clock:     clock,
	}
	d.dispatcher = NewDispatcher(store, publisher, cfg, clock)
	d.dlq = NewDLQ(store, clock)
	d.cleaner = NewCleaner(store, cfg.Retention, cfg.CleanupInterval, clock)
	return d
}

// Run implements Outbox: business writes and enqueues commit atomically.
func (o *DefaultOutbox) Run(ctx context.Context, fn func(tx Tx) error) error {
	return o.store.Transaction(ctx, func(stx StoreTx) error {
		v := &txView{storeTx: stx, idGen: o.idGen, clock: o.clock, maxSize: o.cfg.MaxOutboxSize}
		return fn(v)
	})
}

// Start launches dispatcher and cleaner background loops.
func (o *DefaultOutbox) Start(ctx context.Context) {
	o.cleaner.Start(ctx)
	o.dispatcher.Start(ctx)
}

// Stop gracefully stops all background loops.
func (o *DefaultOutbox) Stop() {
	o.dispatcher.Stop()
	o.cleaner.Stop()
}

// Dispatcher exposes the delivery relay.
func (o *DefaultOutbox) Dispatcher() *Dispatcher { return o.dispatcher }

// DLQ exposes the dead-letter manager.
func (o *DefaultOutbox) DLQ() *DLQ { return o.dlq }

// Cleaner exposes the retention cleaner.
func (o *DefaultOutbox) Cleaner() *Cleaner { return o.cleaner }

// txView adapts a StoreTx to the user-facing Tx interface.
type txView struct {
	storeTx StoreTx
	idGen   IDGenerator
	clock   Clock
	maxSize int
}

func (t *txView) Enqueue(ctx context.Context, stream, msgType string, payload []byte, opts ...EnqueueOption) (string, error) {
	if stream == "" {
		return "", errors.New("outbox: stream must not be empty")
	}
	eo := enqueueOptions{}
	for _, opt := range opts {
		opt(&eo)
	}
	id := eo.id
	if id == "" {
		var err error
		if id, err = t.idGen.NewID(); err != nil {
			return "", err
		}
	}
	now := t.clock.Now()
	msg := &Message{
		ID:          id,
		Stream:      stream,
		Type:        msgType,
		Payload:     append([]byte(nil), payload...),
		Metadata:    cloneMetadata(eo.metadata),
		Status:      StatusPending,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	// Enforce the backlog bound inside the same transaction: business writes
	// and the message are rejected together, so no partial state is committed.
	active, err := t.storeTx.ActiveCount(ctx)
	if err != nil {
		return "", err
	}
	if active >= t.maxSize {
		return "", ErrOutboxFull
	}
	if err := t.storeTx.InsertMessage(ctx, msg); err != nil {
		return "", err
	}
	return id, nil
}

func (t *txView) Put(ctx context.Context, key string, value []byte) error {
	return t.storeTx.PutKV(ctx, key, append([]byte(nil), value...))
}

func (t *txView) Delete(ctx context.Context, key string) error {
	return t.storeTx.DeleteKV(ctx, key)
}

// WithID sets an explicit, caller-provided idempotency key on a message.
func WithID(id string) EnqueueOption {
	return func(o *enqueueOptions) { o.id = id }
}

// WithMetadata attaches metadata to an enqueued message.
func WithMetadata(md map[string]string) EnqueueOption {
	return func(o *enqueueOptions) { o.metadata = cloneMetadata(md) }
}

func cloneMetadata(md map[string]string) map[string]string {
	if len(md) == 0 {
		return nil
	}
	cp := make(map[string]string, len(md))
	for k, v := range md {
		cp[k] = v
	}
	return cp
}
