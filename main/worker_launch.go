package main

import (
	"fmt"
	"os"
	"plugin"

	"github.com/bgilby59/distributed-grep/mapreduce"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: worker_launch application_file.so")
	}

	mapfunc, reducefunc := LoadMR(os.Args[1])

	mapreduce.Worker(mapfunc, reducefunc)
}

func LoadMR(application_file string) (func(string, string) mapreduce.KeyValue, func(string, []string) string) {
	app, err := plugin.Open(application_file)
	if err != nil {
		fmt.Printf("error opening application: %v", err)
	}

	mapfunc_sym, _ := app.Lookup("Map")
	reducefunc_sym, _ := app.Lookup("Reduce")

	mapfunc := mapfunc_sym.(func(string, string) []mapreduce.KeyValue)
	reducefunc := reducefunc_sym.(func(string, []string) string)

	return mapfunc, reducefunc
}
