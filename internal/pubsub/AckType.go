package pubsub

// AckType is the "enum" a handler returns to tell the subscriber
// what should happen to the delivery that carried the message.
type AckType int

const (
	// Ack acknowledges the delivery, removing it from the queue.
	Ack AckType = iota
	// NackRequeue negatively acknowledges the delivery and requeues it.
	NackRequeue
	// NackDiscard negatively acknowledges the delivery without requeueing it.
	NackDiscard
)
