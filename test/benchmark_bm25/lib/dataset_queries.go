//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
)

type Queries []string

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

		q = append(q, queryStr)
	}

	return q, nil
}
