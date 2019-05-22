package stopwords

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
