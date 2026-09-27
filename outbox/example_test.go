package outbox_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guoyyit/transactional-outbox-go/outbox"
	"github.com/guoyyit/transactional-outbox-go/store/memory"
)

// Example shows the canonical usage: business state change and outbound message
// committed in one local transaction, delivered asynchronously by the relay.
func Example() {
	ctx := context.Background()
	store := memory.New()

	publisher := outbox.PublisherFunc(func(ctx context.Context, m *outbox.Message) error {
		// The downstream (or an edge broker) deduplicates on the stable m.ID.
		fmt.Printf("deliver stream=%s seq=%d type=%s id=%s body=%s\n",
			m.Stream, m.Seq, m.Type, m.ID, string(m.Payload))
		return nil
	})

	cfg := outbox.DefaultConfig()
	ob := outbox.New(store, publisher, outbox.WithConfig(cfg))

	// 1) Atomic business write + outbox message.
	err := ob.Run(ctx, func(tx outbox.Tx) error {
		if err := tx.Put(ctx, "orders/42", []byte("PAID")); err != nil {
			return err
		}
		_, err := tx.Enqueue(ctx, "orders", "order.paid",
			[]byte(`{"orderId":42}`),
			outbox.WithID("idempotency-key-42"), // explicit dedupe key
			outbox.WithMetadata(map[string]string{"trace": "abc"}),
		)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	// 2) Independent relay delivers committed messages.
	ob.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	ob.Stop()

	// 3) Inspect / replay dead letters.
	dead, _ := ob.DLQ().List(ctx, "orders", 10, 0)
	fmt.Printf("dead letters: %d\n", len(dead))
	// Output:
	// deliver stream=orders seq=1 type=order.paid id=idempotency-key-42 body={"orderId":42}
	// dead letters: 0
}
