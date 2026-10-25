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

	_, err := rapide.Dial(ctx, "localhost:3000")
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
