package main

import (
	"log"

	"github.com/maklean/rapide"
)

func main() {
	conn, err := rapide.Dial("localhost:3000")
	if err != nil {
		log.Fatal("Failed to dial localhost:3000 :", err)
	}

	message := "Hello, World!"
	err = conn.Send([]byte(message))

	if err != nil {
		log.Fatal("Failed to send message to listener:", err)
	}
}
