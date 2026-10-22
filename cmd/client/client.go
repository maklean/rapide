package main

import (
	"fmt"
	"log"
	"math/rand/v2"

	"github.com/maklean/rapide"
)

func main() {
	for range 10 {
		go connectToListener(rand.IntN(1000000))
	}

	select {}
}

func connectToListener(startNum int) {
	conn, err := rapide.Dial("localhost:3000")
	if err != nil {
		log.Fatalf("Failed to connect to 'localhost:3000': %v", err)
	}

	for i := range 10 {
		err = conn.Send(fmt.Appendf(nil, "RELIABLE MESSAGE %d", startNum+i), true)
		if err != nil {
			log.Fatalf("Failed to send message to server: %v", err)
		}
	}

	for i := range 10 {
		err = conn.Send(fmt.Appendf(nil, "UNRELIABLE MESSAGE %d", startNum+i), false)
		if err != nil {
			log.Fatalf("Failed to send message to server: %v", err)
		}
	}
	log.Println("Sent messages to server.")
}
