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

	log.Printf("got connection '%s', connected to '%s', closing it...", conn.LocalAddr().String(), conn.RemoteAddr().String())
	conn.Close()
	log.Println("closed connection")
}
