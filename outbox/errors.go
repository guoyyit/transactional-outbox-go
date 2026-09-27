package outbox

import "errors"

// ErrLeaseLost is returned by Mark* operations when the caller's lease token
// no longer matches the stored row. It means crash recovery already reassigned
// the message, so the caller must discard its delivery result.
var ErrLeaseLost = errors.New("outbox: delivery lease lost")

// ErrNotFound is returned when a queried message does not exist.
var ErrNotFound = errors.New("outbox: message not found")
