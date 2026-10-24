package main

import (
	"log"
	"time"

	"github.com/maklean/rapide"
)

func main() {
	listener, err := rapide.Listen("localhost:3000")
	if err != nil {
		log.Fatalf("Failed to start listening on 'localhost:3000': %v", err)
	}

	log.Println("Server started on localhost:3000...")

	for range 2 {
		conn, err := listener.Accept()

		if err != nil {
			continue
		}

		log.Println("ACCEPTED CLIENT")
		go handleConnection(conn)
	}

	time.Sleep(5 * time.Second)
	listener.Close()
	log.Println("closed listener")
}

func handleConnection(conn *rapide.Connection) {
	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("There was an error trying to read from the client: %v", err)
		return
	}

	log.Printf("Got buffer from client: %s", string(buf[:n]))

	err = conn.Send(buf[:n], true)
	if err != nil {
		log.Fatalf("failed to send buffer to connection")
	}
}
