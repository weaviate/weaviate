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
	"os"
	"path/filepath"
	"strconv"
)

type Query struct {
	Query       string
	MatchingIds []int
}

type Queries []Query

func ParseQueries(ds Dataset, limit int) (Queries, error) {
	p := filepath.Join(ds.Path, "queries.jsonl")
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open queries file at %s: %w", p, err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	q := Queries{}
	obj := map[string]interface{}{}
	for scanner.Scan() {
		if limit > 0 && len(q) >= limit {
			break
		}

		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			return nil, err
		}

		queryStr, ok := obj[ds.Queries.Property].(string)
		if !ok {
			return nil, fmt.Errorf("property %s is not a string: %T",
				ds.Queries.Property, obj[ds.Queries.Property])
		}

		var matchingIds []int
		if ds.Queries.MatchingResults != "" {
			matchingIdsInterface, ok := obj[ds.Queries.MatchingResults].([]interface{})
			if !ok {
				return nil, fmt.Errorf("property %s is not a []interface{}]: %T",
					ds.Queries.MatchingResults, obj[ds.Queries.MatchingResults])
			}
			matchingIds = make([]int, len(matchingIdsInterface))
			for i, val := range matchingIdsInterface {
				// docIds can be provided as strings or float arrays
				valStr, ok := val.(string)
				if ok {
					valInt, err := strconv.Atoi(valStr)
					if err != nil {
						return nil, err
					}
					matchingIds[i] = valInt

				} else {
					valFloat, ok := val.(float64)
					if !ok {
						return nil, fmt.Errorf("matching Id %v is not a float: %v", i, matchingIdsInterface)
					}
					matchingIds[i] = int(valFloat)
				}

			}

		}

		q = append(q, Query{Query: queryStr, MatchingIds: matchingIds})
	}

	return q, nil
}
