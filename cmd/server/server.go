package main

import (
	"log"

	"github.com/maklean/rapide"
)

func main() {
	_, err := rapide.Listen("localhost:3000")
	if err != nil {
		log.Fatalf("Failed to start listening on 'localhost:3000': %v", err)
	}

	log.Println("Server started on localhost:3000...")

	select {}
}
