package pubsub

import (
	"bytes"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(body []byte) (T, error) {
		var msg T
		err := gob.NewDecoder(bytes.NewReader(body)).Decode(&msg)
		return msg, err
	})
}
