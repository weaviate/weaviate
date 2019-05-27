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
 */package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/semi-technologies/weaviate/contextionary"
)

func searchString(word string, c11y contextionary.Contextionary) {
	words := strings.Split(word, " ")

	var usableWords []string
	var vectors []contextionary.Vector
	var weights []float32

	for _, word := range words {
		if isStopWord(word) {
			continue
		}

		itemIndex := c11y.WordToItemIndex(word)
		if ok := itemIndex.IsPresent(); !ok {
			log.Fatalf("the word %s is not in the c11y", word)
		}

		vector, err := c11y.GetVectorForItemIndex(itemIndex)
		if err != nil {
			log.Fatalf("could not get vector for word '%s': %v", word, err)
		}

		usableWords = append(usableWords, word)
		vectors = append(vectors, *vector)
		weights = append(weights, 1.0)
	}

	stopWordsRatio := float32((len(words) - len(usableWords))) / float32(len(words))
	fmt.Printf("Original Search Term: %s\n", word)
	fmt.Printf("After stop word removal: %s (%2.0f%% removed)\n", strings.Join(usableWords, " "), stopWordsRatio*100)
	fmt.Printf("\n")

	centroid, err := contextionary.ComputeWeightedCentroid(vectors, weights)
	fatal(err)

	search(centroid.ToArray())
	fmt.Printf("\n\n")
}

func search(v []float32) {
	body := fmt.Sprintf(`{
  "query": {
    "function_score": {
		  "query": {
			  "bool": {
				  "filter": {
						"match": {
						  "sampleBoolProp": false
						}
					}
				}
			},
      "boost_mode": "replace",
      "script_score": {
        "script": {
          "inline": "binary_vector_score",
          "lang": "knn",
          "params": {
            "cosine": false,
            "field": "embedding_vector",
            "vector": [
						%s
             ]
          }
        }
      }
    }
  },
  "size": 3
} `, printVector(v))

	req, _ := http.NewRequest("GET", "http://localhost:9900/documents/_search", bytes.NewReader([]byte(body)))
	res, err := (&http.Client{}).Do(req)
	if err != nil {
		panic(err)
	}

	if res.StatusCode != 200 {
		bb, _ := ioutil.ReadAll(res.Body)
		panic(fmt.Errorf("status is %d: %s", res.StatusCode, bb))
	}

	defer res.Body.Close()
	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	var eres elasticResult
	err = json.Unmarshal(bytes, &eres)
	if err != nil {
		panic(err)
	}

	for i, hit := range eres.Hits.Hits {
		content := firstChars(hit.Source.Content, 120)
		fmt.Printf("\n\tNo: %d\tScore: %2.3f\tName: %s\n\t  Content: %s\n", i, hit.Score, hit.Source.Name, content)
	}
}

type elasticResult struct {
	Hits elasticHits `json:"hits"`
}

type elasticHits struct {
	Hits []elasticHit `json:"hits"`
}

type elasticHit struct {
	Score  float32  `json:"_score"`
	Source document `json:"_source"`
}

func firstChars(input string, limit int) string {
	if len(input) < limit {
		return input
	}
	return input[:limit] + "..."
}
