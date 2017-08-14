// This tool generates the special-case code for a small number of watchers
// which runs all the watches in a single select vs. needing to spawn a
// goroutine for each one.
package main

import (
	"fmt"
	"os"
	"text/template"
)

// aFew should be set to the number of channels to special-case for. Setting
// this is a tradeoff for how big the slice is for the smallest watch set that
// we see in practice vs. the number of goroutines we save when dealing with a
// large number of watches. This was tuned with BenchmarkWatch to get setup
// time for a watch with 1024 channels under 100 us on a 2.7 GHz Core i5.
const aFew = 32

// source is the template we use to generate the source file.
const source = `//go:generate sh -c "go run watch-gen/main.go >watch_few.go"
package memdb

import(
	"time"
)

// aFew gives how many watchers this function is wired to support. You must
// always pass a full slice of this length, but unused channels can be nil.
const aFew = {{len .}}

// watchFew is used if there are only a few watchers as a performance
// optimization.
func watchFew(ch []<-chan struct{}, timeoutCh <-chan time.Time) bool {
	select {
{{range $i, $unused := .}}
	case <-ch[{{printf "%d" $i}}]:
		return false
{{end}}
	case <-timeoutCh:
		return true
	}
}
`

// render prints the template to stdout.
func render() error {
	tmpl, err := template.New("watch").Parse(source)
	if err != nil {
		return err
	}
	if err := tmpl.Execute(os.Stdout, make([]struct{}, aFew)); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := render(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
