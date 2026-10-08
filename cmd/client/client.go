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
		message := fmt.Appendf(nil, "Message %d", i)

		err = conn.Send(message)
		if err != nil {
			fmt.Printf("Failed to send message to server: %v", err)
		}
	}

	log.Println("Sent message to server.")
}
