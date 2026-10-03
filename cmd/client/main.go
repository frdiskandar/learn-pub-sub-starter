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
func main() {
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(UrlAmqp)
	if err != nil{
		fmt.Printf("err to connect amqp: %v", err)
		return
	}

	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt, syscall.SIGTERM)

	go func(){
		sgn := <- sign
		fmt.Printf("program closed %v", sgn)
		os.Exit(1)
	}()

	username, _ := gamelogic.ClientWelcome()
	qname := routing.PauseKey + "." + username
	pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, qname, routing.PauseKey, "transient")

	select{}
}
