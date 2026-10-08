package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(body []byte) (T, error) {
		var msg T
		err := json.Unmarshal(body, &msg)
		return msg, err
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, que := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	ch.Qos(10, 0, false)
	deliveries, err := ch.Consume(que.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register a consumer, %v", err)
	}

	go func() {
		for d := range deliveries {
			msg, err := unmarshaller(d.Body)
			if err != nil {
				fmt.Printf("failed to unmarshal delivery, %v \n", err)
				if err := d.Ack(false); err != nil {
					fmt.Printf("failed to ack delivery, %v \n", err)
				}
				continue
			}

			switch handler(msg) {
			case NackRequeue:
				fmt.Println("debug: NackRequeue -> msg.Nack(false, true)")
				if err := d.Nack(false, true); err != nil {
					fmt.Printf("failed to nack delivery, %v \n", err)
				}
			case NackDiscard:
				fmt.Println("debug: NackDiscard -> msg.Nack(false, false)")
				if err := d.Nack(false, false); err != nil {
					fmt.Printf("failed to nack delivery, %v \n", err)
				}
			case Ack:
				fmt.Println("debug: Ack -> msg.Ack(false)")
				if err := d.Ack(false); err != nil {
					fmt.Printf("failed to ack delivery, %v \n", err)
				}
			}
		}
	}()

	return nil
}
