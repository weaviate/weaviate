package lib

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Queries []string

func ParseQueries(ds Dataset) (Queries, error) {
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
