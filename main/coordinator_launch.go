package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bgilby59/distributed-grep/mapreduce"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: coordinator_launch inputfiles...\n")
		os.Exit(1)
	}

	m := mapreduce.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
