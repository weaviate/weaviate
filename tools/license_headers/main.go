//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

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
	buildTagRe      *regexp.Regexp
	targetHeader    []byte
)

func init() {
	headerSectionRe = regexp.MustCompile(`^(//.*\n)*\n`)
	buildTagRe = regexp.MustCompile(`^//\s\+build`)
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

	if hasNoHeader(bytes, name) {
		err := extendWithHeader(name, bytes)
		if err != nil {
			return fmt.Errorf("%s: %v", name, err)
		}
		fmt.Printf("🖋️  succesfully created header: %s\n", name)
		return nil
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

func extendWithHeader(name string, content []byte) error {
	target := append(targetHeader, []byte("\n\n")...)
	updated := append(target, content...)
	return ioutil.WriteFile(name, updated, 0)
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
	return ioutil.WriteFile(name, replaced, 0)
}

func hasNoHeader(content []byte, name string) bool {
	h := headerSectionRe.Find(content)
	if h == nil {
		// there is no comment section at all
		return true
	}

	lines := bytes.Split(h, []byte("\n"))
	if len(lines) < 4 {
		// this comment is so short, this is most likely not a header section,
		// let's add a header in front of it instead
		return true
	}

	return false
}

func startsWithBuildTag(content []byte) bool {
	return buildTagRe.Match(content)
}

func fatal(err error) {
	if err != nil {
		log.Fatalf(err.Error())
	}
}
