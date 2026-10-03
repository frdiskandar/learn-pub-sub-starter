package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

var UrlAmqp = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril server...")
	conn, err  := amqp.Dial(UrlAmqp)
	if err != nil{
		fmt.Printf("Err Connection to amqp :%v", err)
		return
	}
	fmt.Println("amqp Connected")
	defer conn.Close()
	signOut := make(chan os.Signal, 1)
	signal.Notify(signOut, os.Interrupt, syscall.SIGTERM)

	go func ()  {
		sig := <- signOut
		fmt.Printf("Program Closeed %v \n", sig)
		os.Exit(1)
	}()

	ch, err := conn.Channel(); if err != nil{
		fmt.Printf("Failed to create Channel %v \n", err)
		return
	}

	if err := ch.ExchangeDeclare(routing.ExchangePerilDirect, "direct", false, true, false, false, nil); err != nil{
		fmt.Printf("Failed to create Exchange %v \n", err)
		return
	}

	que, err := ch.QueueDeclare("pause_test", true, true, false, false, nil)
	if err != nil{
			fmt.Printf("Failed to create Queue %v \n", err)
		return
	}

	err = ch.QueueBind(que.Name, routing.PauseKey, routing.ExchangePerilDirect, false, nil)
	if err != nil{
		fmt.Printf("Failed to Bind Que and Exchange %v \n", err)
		return
	}

	if err := pubsub.PublishJSON(ch,routing.ExchangePerilDirect,
		 routing.PauseKey,  routing.PlayingState{IsPaused: true}); 
		 err != nil{
			fmt.Printf("Failed to Publish %v \n", err)
		return
		 }


	fmt.Println("Press Ctrl + C to exit program")
	select{}
}
