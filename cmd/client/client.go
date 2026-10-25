package main

import (
	"context"
	"log"
	"time"

	"github.com/maklean/rapide"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	conn, err := rapide.Dial(ctx, "localhost:3000")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("got connection '%s', connected to '%s'...", conn.LocalAddr().String(), conn.RemoteAddr().String())

	buf := make([]byte, 1024)

	n, err := conn.Read(buf)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Received data from server: %s", string(buf[:n]))

	err = conn.Send([]byte("Hello, server!"), true)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Sent message back to server")
	select {}

}
