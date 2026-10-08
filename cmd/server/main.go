package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

var UrlAmqp = "amqp://guest:guest@localhost:5672/"

func handlerGameLog(gameLog routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	if err := gamelogic.WriteLog(gameLog); err != nil {
		fmt.Printf("Error: failed to write game log: %v\n", err)
	}
	return pubsub.Ack
}

func main() {
	fmt.Println("Starting Peril server...")
	conn, err := amqp.Dial(UrlAmqp)
	if err != nil {
		fmt.Printf("Err Connection to amqp :%v", err)
		return
	}
	fmt.Println("amqp Connected")
	defer conn.Close()
	signOut := make(chan os.Signal, 1)
	signal.Notify(signOut, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-signOut
		fmt.Printf("Program Closeed %v \n", sig)
		os.Exit(1)
	}()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create Channel %v \n", err)
		return
	}

	if err := ch.ExchangeDeclare(routing.ExchangePerilDirect, "direct", false, false, false, false, nil); err != nil {
		fmt.Printf("Failed to create Exchange %v \n", err)
		return
	}

	que, err := ch.QueueDeclare("pause_test", true, false, false, false, nil)
	if err != nil {
		fmt.Printf("Failed to create Queue %v \n", err)
		return
	}

	err = ch.QueueBind(que.Name, routing.PauseKey, routing.ExchangePerilDirect, false, nil)
	if err != nil {
		fmt.Printf("Failed to Bind Que and Exchange %v \n", err)
		return
	}

	if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect,
		routing.PauseKey, routing.PlayingState{IsPaused: true}); err != nil {
		fmt.Printf("Failed to Publish %v \n", err)
		return
	}


	if err := ch.ExchangeDeclare(routing.ExchangePerilTopic, "topic", true, false, false, false, nil); err != nil{
		fmt.Println("Cannot create exchange")
		return
	}
	if err := pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", "durable", handlerGameLog); err != nil {
		fmt.Printf("Failed to subscribe to game_logs queue %v \n", err)
		return
	}


gamelogic.PrintServerHelp()
gameLoop:
	for {
		playerInput := gamelogic.GetInput()
		if len(playerInput) < 1 {
			continue
		}

		switch playerInput[0] {
		case "pause":

			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect,
				routing.PauseKey, routing.PlayingState{IsPaused: true}); err != nil {
				fmt.Printf("Failed to Publish %v \n", err)
				break gameLoop
			}

		case "resume":
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect,
				routing.PauseKey, routing.PlayingState{IsPaused: false}); err != nil {
				fmt.Printf("Failed to Publish %v \n", err)
				break gameLoop
			}

		case "quit":
			fmt.Println("Quit Program")
			break gameLoop
		
		default:
			fmt.Println("unknown input")
		}
	}

	fmt.Println("Press Ctrl + C to exit program")
	select {}
}
