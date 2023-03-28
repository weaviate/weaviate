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
	"log"
	"bufio"
	"encoding/json"
	"fmt"
	
	"os"
	"path/filepath"
	"strconv"
)

type Query struct {
	QueryIdStr string
	Id int
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

		idStr := obj["queryID"].(string)
		id, _ := strconv.Atoi(idStr)

		q = append(q, Query{Query: queryStr, MatchingIds: matchingIds, Id: id})
	}

	return q, nil
}

/*
{"_id": "3", "title": "", "text": "I'm not saying I don't like the idea of on-the-job training too, but you can't expect the company to do that. Training workers is not their job - they're building software. Perhaps educational systems in the U.S. (or their students) should worry a little about getting marketable skills in exchange for their massive investment in education, rather than getting out with thousands in student debt and then complaining that they aren't qualified to do anything.", "metadata": {}}
*/

type CorpusData struct {
	IdStr      string                 `json:"_id"`
	Id int
	Title   string                 `json:"title"`
	Text    string                 `json:"text"`
	Meta    map[string]interface{} `json:"metadata"`
}

func LoadCorpus(ds Dataset) []CorpusData {
	var corpus []CorpusData
	p := filepath.Join(ds.Path, "corpus.jsonl")
	log.Printf("Loading corpus from %s", p)

	//Open p and read it line-by-line
	file, err := os.Open(p)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var text []byte
	for scanner.Scan() {
		var item CorpusData
		json.Unmarshal(scanner.Bytes(), &item)
		item.Id, _ = strconv.Atoi(item.IdStr)
		corpus = append(corpus, item)
	}

	
	json.Unmarshal(text, &corpus)
	return corpus


}
