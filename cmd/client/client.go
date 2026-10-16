package main

import (
	"fmt"
	"log"

	"github.com/maklean/rapide"
)

func main() {
	conn, err := rapide.Dial("localhost:3000")
	if err != nil {
		log.Fatalf("Failed to connect to 'localhost:3000': %v", err)
	}

	for i := range 10 {
		err = conn.Send(fmt.Appendf(nil, "Message %d", i), true)
		if err != nil {
			log.Fatalf("Failed to send message to server: %v", err)
		}
	}

	for i := range 10 {
		err = conn.Send(fmt.Appendf(nil, "Message %d", i), false)
		if err != nil {
			log.Fatalf("Failed to send message to server: %v", err)
		}
	}

	log.Println("Sent messages to server.")
	conn.Close()
}
