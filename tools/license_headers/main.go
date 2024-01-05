//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar"
)

var (
	headerSectionRe *regexp.Regexp
	buildTagRe      *regexp.Regexp
	targetHeader    []byte
)

func init() {
	headerSectionRe = regexp.MustCompile(`^(//.*\n)*\n`)
	buildTagRe = regexp.MustCompile(`^//go:build`)
	h, err := os.ReadFile("tools/license_headers/header.txt")
	fatal(err)
	targetHeader = bytes.TrimSpace(h)
}

func main() {
	fileNames, err := doublestar.Glob("**/*.go")
	fatal(err)
	for _, name := range fileNames {
		if len := len(name); len > 5 && name[len-6:] == ".pb.go" || strings.HasPrefix(name, "vendor/") {
			continue
		}
		fatal(processSingleFile(name))
	}
}

func processSingleFile(name string) error {
	bytes, err := os.ReadFile(name)
	if err != nil {
		return fmt.Errorf("%s: %v", name, err)
	}

	if hasNoHeader(bytes, name) {
		err := extendWithHeader(name, bytes)
		if err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}
		fmt.Printf("🖋️  successfully created header: %s\n", name)
		return nil
	}

	if headerNeedsUpdate(bytes) {
		err := updateHeader(name, bytes)
		if err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}

		fmt.Printf("👷 successfully updated: %s\n", name)
	} else {
		fmt.Printf("✅ already up to date: %s\n", name)
	}
	return nil
}

func headerNeedsUpdate(content []byte) bool {
	current := headerSectionRe.Find(content)
	return !bytes.Equal(bytes.TrimSpace(current), targetHeader)
}

func extendWithHeader(name string, content []byte) error {
	target := append(targetHeader, []byte("\n\n")...)
	updated := append(target, content...)
	return os.WriteFile(name, updated, 0)
}

func updateHeader(name string, content []byte) error {
	if startsWithBuildTag(content) {
		// the document starts with a build tag, this means we don't replace, but
		// insert the header at pos 0
		return extendWithHeader(name, content)
	}

	// append a newline so we consistently have one newline regardless of the
	// input doc
	target := append(targetHeader, []byte("\n\n")...)
	replaced := headerSectionRe.ReplaceAll(content, target)
	return os.WriteFile(name, replaced, 0)
}

func hasNoHeader(content []byte, name string) bool {
	h := headerSectionRe.Find(content)
	if h == nil {
		// there is no comment section at all
		return true
	}

	lines := bytes.Split(h, []byte("\n"))

	// this comment is so short, this is most likely not a header section,
	// let's add a header in front of it instead
	return len(lines) < 4
}

func startsWithBuildTag(content []byte) bool {
	return buildTagRe.Match(content)
}

func fatal(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}
