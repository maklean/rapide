package main

import (
	"log"

	"github.com/maklean/rapide"
)

func main() {
	listener, err := rapide.Listen("localhost:3000")
	if err != nil {
		log.Fatalf("Failed to start listening on 'localhost:3000': %v", err)
	}

	log.Println("Server started on localhost:3000...")

	for {
		go handleConnection(listener.Accept())
	}
}

func handleConnection(conn *rapide.Connection) {
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("There was an error trying to read from the client: %v", err)
			continue
		}

		log.Printf("Got buffer from client: %s", string(buf[:n]))
	}
}
