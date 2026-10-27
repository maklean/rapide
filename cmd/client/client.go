package main

import (
	"context"
	"fmt"
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

	i := 0

	for {
		err = conn.Send(fmt.Appendf(nil, "Hello Server %d!", i), false)
		if err != nil {
			continue
		}

		i++

		log.Println("Sent message to server")
	}
}
