package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"

	"github.com/bmatcuk/doublestar"
)

var (
	headerSectionRe *regexp.Regexp
	targetHeader    []byte
)

func init() {
	headerSectionRe = regexp.MustCompile(`^(//.*\n)*\n`)
	h, err := ioutil.ReadFile("tools/license_headers/header.txt")
	fatal(err)
	targetHeader = bytes.TrimSpace(h)
}

func main() {
	fileNames, err := doublestar.Glob("**/*.go")
	fatal(err)

	for _, fname := range fileNames {
		fatal(processSingleFile(fname))
	}

}

func processSingleFile(name string) error {
	bytes, err := ioutil.ReadFile(name)
	if err != nil {
		return fmt.Errorf("%s: %v", name, err)
	}

	if headerNeedsUpdate(bytes) {
		err := updateHeader(name, bytes)
		if err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}

		fmt.Printf("👷 succesfully updated: %s\n", name)
	} else {
		fmt.Printf("✅ already up to date: %s\n", name)
	}
	return nil
}

func headerNeedsUpdate(content []byte) bool {
	current := headerSectionRe.Find(content)
	if bytes.Equal(bytes.TrimSpace(current), targetHeader) {
		return false
	}

	return true
}

func updateHeader(name string, content []byte) error {
	// append a newline so we consistently have one newline regardless of the
	// input doc
	target := append(targetHeader, []byte("\n")...)
	replaced := headerSectionRe.ReplaceAll(content, target)
	return ioutil.WriteFile(name, replaced, 0)
}

func fatal(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}
