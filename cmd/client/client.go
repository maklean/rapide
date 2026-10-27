package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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
		reliable := rand.Intn(2) == 1
		rStr := "[unreliable]"
		if reliable {
			rStr = "[reliable]"
		}

		err = conn.Send(fmt.Appendf(nil, "%s Hello Server %d!", rStr, i), reliable)
		if err != nil {
			continue
		}

		i++

		log.Println("Sent message to server")
	}
}
