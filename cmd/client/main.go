package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutcomeMakeWar:
			recognition := gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			}
			key := routing.WarRecognitionsPrefix + "." + gs.GetUsername()
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, recognition); err != nil {
				fmt.Printf("Error: failed to publish war: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			if err := publishGameLog(ch, gs.GetUsername(), message); err != nil {
				fmt.Printf("Error: failed to publish game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			message := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			if err := publishGameLog(ch, gs.GetUsername(), message); err != nil {
				fmt.Printf("Error: failed to publish game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Printf("Error: unknown war outcome: %v\n", outcome)
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, username, message string) error {
	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, routing.GameLog{
		CurrentTime: time.Now(),
		Message:     message,
		Username:    username,
	})
}

var UrlAmqp = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril client...")
	conn, err := amqp.Dial(UrlAmqp)
	if err != nil {
		fmt.Printf("err to connect amqp: %v", err)
		return
	}

	sign := make(chan os.Signal, 1)
	signal.Notify(sign, os.Interrupt, syscall.SIGTERM)

	go func() {
		sgn := <-sign
		fmt.Printf("program closed %v", sgn)
		os.Exit(1)
	}()

	username, _ := gamelogic.ClientWelcome()
	qname := routing.PauseKey + "." + username
	gs := gamelogic.NewGameState(username)

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, qname, routing.PauseKey, "transient", handlerPause(gs)); err != nil {
		fmt.Printf("Failed to subscribe to pause queue: %v\n", err)
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create channel: %v\n", err)
		return
	}

	moveQueueName := routing.ArmyMovesPrefix + "." + username
	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, moveQueueName, routing.ArmyMovesPrefix+".*", "transient", handlerMove(gs, ch)); err != nil {
		fmt.Printf("Failed to subscribe to moves queue: %v\n", err)
		return
	}

	if err := pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", "durable", handlerWar(gs, ch)); err != nil {
		fmt.Printf("Failed to subscribe to war queue: %v\n", err)
		return
	}

gameLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gs.CommandSpawn(words); err != nil {
				fmt.Printf("Error: %v\n", err)
			}
		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				break
			}
			fmt.Println("Move successful!")
			if err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, mv); err != nil {
				fmt.Printf("Error: failed to publish move: %v\n", err)
				break
			}
			fmt.Println("Move published successfully!")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("Error: usage: spam <count>")
				break
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Printf("Error: %s is not a valid count\n", words[1])
				break
			}
			published := 0
			for i := 0; i < n; i++ {
				if err := publishGameLog(ch, username, gamelogic.GetMaliciousLog()); err != nil {
					fmt.Printf("Error: failed to publish log: %v\n", err)
					break
				}
				published++
			}
			fmt.Printf("Published %v game logs!\n", published)
		case "quit":
			gamelogic.PrintQuit()
			break gameLoop
		default:
			fmt.Println("Error: unknown command. Type 'help' for a list of commands.")
		}
	}
}
