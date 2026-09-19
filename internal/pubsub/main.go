package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	TypeDurable SimpleQueueType = iota
	TypeTransient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
	if err != nil {
		return err
	}

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	connCh, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue := amqp.Queue{}
	switch queueType {
	case TypeDurable:
		queue, err = connCh.QueueDeclare(queueName, true, false, false, false, nil)
	case TypeTransient:
		queue, err = connCh.QueueDeclare(queueName, false, true, true, false, nil)
	default:
		return nil, amqp.Queue{}, fmt.Errorf("Couldn't extract queue type")
	}
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = connCh.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return connCh, queue, nil
}
