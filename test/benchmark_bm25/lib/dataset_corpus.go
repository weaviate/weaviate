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

package lib

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

type Corpi []Corpus

type Corpus map[string]string

// TODO: just loading the whole corpus into memory isn't very efficient, this
// could be improved by iterating one object at a time, e.g. with a callback
func ParseCorpi(ds Dataset, multiply int) (Corpi, error) {
	p := filepath.Join(ds.Path, "corpus.jsonl")
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open queries file at %s: %w", p, err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	// we need reproducible random numbers to be able to compare different runs. This only needs to be stable for a
	// given version of the code. If it produces different numbers after a dependency update it doesn't matter.
	r := rand.New(rand.NewSource(9))

	c := Corpi{}
	for scanner.Scan() {
		obj := map[string]interface{}{}
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			return nil, err
		}

		corp := Corpus{}
		for _, prop := range ds.Corpus.IndexedProperties {
			propStr, ok := obj[prop].(string)
			if !ok {
				return nil, fmt.Errorf("indexed property %s is not a string: %T",
					prop, obj[prop])
			}

			corp[SanitizePropName(prop)] = propStr
			for i := 1; i < multiply; i++ {
				newName := fmt.Sprintf("%s_copy_%d", SanitizePropName(prop), i)
				var propString string
				if len(c) > 1 { // get the content of the property from another object, to get more varied results
					otherObjectIndex := r.Intn(len(corp))
					propString = c[otherObjectIndex][SanitizePropName(prop)]
				} else {
					propString = propStr
				}
				corp[newName] = propString
			}
		}

		for _, prop := range ds.Corpus.UnindexedProperties {
			propStr, ok := obj[prop].(string)
			if !ok {
				return nil, fmt.Errorf("unindexed property %s is not a string: %T",
					prop, obj[prop])
			}

			corp[SanitizePropName(prop)] = propStr
		}

		c = append(c, corp)
	}

	return c, nil
}
