package lib

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Corpi []Corpus

type Corpus struct {
	Indexed   map[string]interface{}
	Unindexed map[string]interface{}
}

// TODO: just loading the whole corpus into memory isn't very efficient, this
// could be improved by iterating one object at a time, e.g. with a callback
func ParseCorpi(ds Dataset) (Corpi, error) {
	p := filepath.Join(ds.Path, "corpus.jsonl")
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open queries file at %s: %w", p, err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	c := Corpi{}
	for scanner.Scan() {
		obj := map[string]interface{}{}
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			return nil, err
		}

		corp := Corpus{
			Indexed:   map[string]interface{}{},
			Unindexed: map[string]interface{}{},
		}

		for _, prop := range ds.Corpus.IndexedProperties {
			propStr, ok := obj[prop].(string)
			if !ok {
				return nil, fmt.Errorf("indexed property %s is not a string: %T",
					prop, obj[prop])
			}

			corp.Indexed[prop] = propStr
		}

		for _, prop := range ds.Corpus.UnindexedProperties {
			propStr, ok := obj[prop].(string)
			if !ok {
				return nil, fmt.Errorf("unindexed property %s is not a string: %T",
					prop, obj[prop])
			}

			corp.Unindexed[prop] = propStr
		}

		c = append(c, corp)
	}

	return c, nil
}
