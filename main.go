// go:generate protoc --go_out=pb/ proto/*.proto
package main

import (
	"log"
	"time"

	"github.com/borud/sst/pending"
)

var data = []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxThis is a test")

const numreps = 1000000

func main() {
	start := time.Now()

	p, err := pending.New("tx.log")
	if err != nil {
		log.Fatal(err)
	}

	var count int64
	for i := 0; i < numreps; i++ {
		err := p.Add(count, time.Now(), 1, data)
		if err != nil {
			log.Fatal(err)
		}

		count++

	}

	count = 0
	for i := 0; i < numreps; i++ {
		err := p.Commit(count, time.Now())
		if err != nil {
			log.Fatal(err)
		}
		count++
	}

	p.Close()

	stop := time.Now()
	elapsed := stop.Sub(start)
	log.Printf("Elapsed seconds : %.3f", elapsed.Seconds())
	log.Printf("Ops /sec        : %.3f", numreps/elapsed.Seconds())
}
