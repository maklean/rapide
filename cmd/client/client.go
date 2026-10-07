package main

import (
	"fmt"
	"log"

	"github.com/maklean/rapide"
)

func main() {
	conn, err := rapide.Dial("localhost:3000")
	if err != nil {
		log.Fatal("Failed to dial localhost:3000 :", err)
	}

	for i := range 10 {
		err = conn.Send([]byte(fmt.Sprintf("Message %d", i+1)), true)

		if err != nil {
			log.Fatal("Failed to send message to listener:", err)
		}
	}
}
