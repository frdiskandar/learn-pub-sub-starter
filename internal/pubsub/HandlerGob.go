package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(val); err != nil {
		return fmt.Errorf("failed to gob encode, : %v", err)
	}

	err := ch.PublishWithContext(context.Background(),
		exchange, key, false, false, amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		})

	return err
}
