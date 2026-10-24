package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	"github.com/maklean/rapide"
)

func main() {
	for range 2 {
		go connectToListener(rand.IntN(1000000))
	}

	time.Sleep(5 * time.Second)
}

func connectToListener(startNum int) {
	conn, err := rapide.Dial("localhost:3000")
	if err != nil {
		return
	}

	defer conn.Close()

	err = conn.Send(fmt.Appendf(nil, "RELIABLE MESSAGE %d", startNum), true)
	if err != nil {
		return
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	log.Println("Got: ", string(buf[:n]))
}
