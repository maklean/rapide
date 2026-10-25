package main

import (
	"log"

	"github.com/maklean/rapide"
)

const ServerAddress = "localhost:3000"

func main() {
	_, err := rapide.Listen(ServerAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Started listening at %s...", ServerAddress)

	select {}
}
