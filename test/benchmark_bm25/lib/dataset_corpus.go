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

type Corpi struct {
	Data     []Corpus
	scanner  *bufio.Scanner
	r        *rand.Rand
	ds       Dataset
	multiply int
}

type Corpus map[string]string

func ParseCorpi(ds Dataset, multiply int) (*Corpi, error) {
	p := filepath.Join(ds.Path, "corpus.jsonl")
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open queries file at %s: %w", p, err)
	}
	scanner := bufio.NewScanner(f)
	// we need reproducible random numbers to be able to compare different runs. This only needs to be stable for a
	// given version of the code. If it produces different numbers after a dependency update it doesn't matter.
	r := rand.New(rand.NewSource(9))

	c := &Corpi{
		Data:     []Corpus{},
		scanner:  scanner,
		r:        r,
		ds:       ds,
		multiply: multiply,
	}

	return c, nil
}

func (c *Corpi) Next(count int) error {
	i := 0

	c.Data = []Corpus{}

	for i < count {

		if !c.scanner.Scan() && c.scanner.Err() != nil {
			return c.scanner.Err()
		}

		obj := map[string]interface{}{}
		if err := json.Unmarshal(c.scanner.Bytes(), &obj); err != nil {
			return err
		}

		corp := Corpus{}
		for _, prop := range c.ds.Corpus.IndexedProperties {
			propStr, ok := obj[prop].(string)
			if !ok {
				return fmt.Errorf("indexed property %s is not a string: %T",
					prop, obj[prop])
			}

			corp[SanitizePropName(prop)] = propStr
			for i := 1; i < c.multiply; i++ {
				newName := fmt.Sprintf("%s_copy_%d", SanitizePropName(prop), i)
				var propString string
				if len(c.Data) > 1 { // get the content of the property from another object, to get more varied results
					otherObjectIndex := c.r.Intn(len(corp))
					propString = c.Data[otherObjectIndex][SanitizePropName(prop)]
				} else {
					propString = propStr
				}
				corp[newName] = propString
			}
		}

		for _, prop := range c.ds.Corpus.UnindexedProperties {
			propStr, ok := obj[prop].(string)
			if !ok {
				propStr = fmt.Sprintf("%v", obj[prop])
			}

			corp[SanitizePropName(prop)] = propStr
		}

		c.Data = append(c.Data, corp)
		i++
	}

	return nil
}
