package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bgilby59/distributed-grep/mapreduce"
)

var pattern string = ""

func Map(filename string, contents string) []mapreduce.KeyValue {
	kva := make([]mapreduce.KeyValue, 0)

	// 1: split contents into lines
	lines := strings.Split(contents, "\n")

	// 2. try to match lines to given grep pattern
	for line_number, line := range lines {
		matched, _ := regexp.Match(pattern, []byte(line))

		// 3. if match return line
		if matched {
			k := fmt.Sprintf("%s (line number #%v)", filename, line_number+1)
			v := line
			kv := mapreduce.KeyValue{Key: k, Value: v}
			kva = append(kva, kv)
		}
	}

	// output key is Sprintf("%s (line number #%v)", filename, line_number)
	// output value is matched line or empty string

	return kva
}

func Reduce(key string, values []string) string {
	return values[0]
}
