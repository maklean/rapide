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

	for range 2 {
		client, err := listener.Accept(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Accepted client: %s", client.RemoteAddr().String())
	}

	time.Sleep(2 * time.Second)

	listener.Close()

	select {}
}
