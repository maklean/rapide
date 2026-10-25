package main

import (
	"context"
	"log"
	"time"

	"github.com/maklean/rapide"
)

const ServerAddress = "localhost:3000"

func main() {
	listener, err := rapide.Listen(ServerAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Started listening at %s...", listener.Addr().String())

	client, err := listener.Accept(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	log.Println("got client, closing it...")
	err = client.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("closed client...")

	time.Sleep(5 * time.Second)
	listener.Close()
}
