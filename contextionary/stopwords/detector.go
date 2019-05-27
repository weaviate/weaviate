/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package stopwords

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// Detector can be used to detect whether a word is a stopword
type Detector struct {
	lookup map[string]int
}

type stopWordDoc struct {
	Language string   `json:"language"`
	Words    []string `json:"words"`
}

// NewFromFile creates an in-memory stopword detector based on a file read once
// at init time
func NewFromFile(path string) (*Detector, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("could not open file at %s: %v", path, err)
	}

	fileBytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("could not read file contents: %v", err)
	}

	var doc stopWordDoc
	err = json.Unmarshal(fileBytes, &doc)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal json: %v", err)
	}

	lookup := buildLookupMap(doc.Words)

	return &Detector{
		lookup: lookup,
	}, nil
}

// IsStopWord returns true on stop words, false on all other words
func (d *Detector) IsStopWord(word string) bool {
	if _, ok := d.lookup[word]; ok {
		return true
	}

	return false
}

func buildLookupMap(words []string) map[string]int {
	lookup := map[string]int{}
	for _, word := range words {
		lookup[word] = 1
	}

	return lookup
}
