package outbox_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	outbox "github.com/guoyyit/transactional-outbox-go"
)

// Example demonstrates the core flow: write business state and an outbox
// message in one local transaction, then let the dispatcher deliver it.
func Example() {
	store, err := outbox.OpenSQLite(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	publisher := outbox.PublisherFunc(func(ctx context.Context, m *outbox.Message) error {
		fmt.Printf("publish stream=%s seq=%d payload=%s\n", m.Stream, m.Seq, m.Payload)
		return nil
	})

	ob, err := outbox.New(store, publisher, outbox.Config{})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := ob.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer ob.Stop(context.Background())

	// Business write + message enqueue commit or roll back together.
	err = ob.Write(ctx, func(tx outbox.Tx) error {
		sqlTx := tx.Raw().(*sql.Tx)
		if _, err := sqlTx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS orders (id TEXT, amount INTEGER)`); err != nil {
			return err
		}
		if _, err := sqlTx.ExecContext(ctx, `INSERT INTO orders VALUES ('o-1', 4200)`); err != nil {
			return err
		}
		return tx.Enqueue(ctx, &outbox.Message{
			Stream:  "orders:o-1", // per-aggregate ordering
			Topic:   "order.created",
			Key:     "o-1",
			Payload: []byte(`{"amount":4200}`),
		})
	})
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond) // let the dispatcher deliver
	// Output:
	// publish stream=orders:o-1 seq=1 payload={"amount":4200}
}
