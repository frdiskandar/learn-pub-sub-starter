package pubsub

import (
	"github.com/bootdotdev/learn-pub-sub-starter/internal/helper"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient",
) (*amqp.Channel, amqp.Queue){

	ch, err := conn.Channel()
	if err != nil{
		helper.HandleError("connection err", err, true)
	}

	dtype := true
	autodel := false
	exclusive := false

	if queueType == "transient"{
		dtype = !dtype
		autodel = !autodel
		exclusive = !exclusive
	}

	que, err := ch.QueueDeclare(queueName, dtype, autodel, exclusive, false, amqp.Table{"x-dead-letter-exchange": routing.ExchangeDlx})
	if err != nil{
		helper.HandleError("cannot create Quee", err, true)
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
		if err != nil{
		helper.HandleError("cannot bind Quee", err, true)
	}

	return ch, que
}