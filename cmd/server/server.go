package main

import (
	"fmt"
	"log"

	"github.com/maklean/rapide"
)

func main() {
	_, err := rapide.Listen("localhost:3000")
	if err != nil {
		log.Fatalln("Failed to listen at localhost:3000 :", err)
	}

	fmt.Println("Running listener on localhost:3000...")

	select {}
}
