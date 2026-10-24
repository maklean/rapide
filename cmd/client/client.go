package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/maklean/rapide"
)

func main() {
	var wg sync.WaitGroup

	for range 2 {
		wg.Add(1)
		go connectToListener(rand.IntN(1000000), &wg)
	}

	wg.Wait()
}

func connectToListener(startNum int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := rapide.Dial("localhost:3000")
	if err != nil {
		return
	}

	defer conn.Close()

	err = conn.Send(fmt.Appendf(nil, "RELIABLE MESSAGE %d", startNum), true)
	if err != nil {
		return
	}

	time.Sleep(10 * time.Second)

	log.Println("Sent messages to server.")
}
