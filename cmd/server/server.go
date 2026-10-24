package main

import (
	"log"
	"sync"

	"github.com/maklean/rapide"
)

func main() {
	var wg sync.WaitGroup

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
		wg.Add(1)
		go handleConnection(conn, &wg)
	}

	wg.Wait()
	listener.Close()
	log.Println("closed listener")
}

func handleConnection(conn *rapide.Connection, wg *sync.WaitGroup) {
	defer wg.Done()

	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		log.Printf("There was an error trying to read from the client: %v", err)
		return
	}

	log.Printf("Got buffer from client: %s", string(buf[:n]))
}
