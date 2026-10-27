package main

import (
	"context"
	"log"

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

	log.Printf("Accepted client: %s", client.RemoteAddr().String())

	buf := make([]byte, 1024)

	for {
		n, err := client.Read(buf)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Received data from client: %s", string(buf[:n]))
	}
}
