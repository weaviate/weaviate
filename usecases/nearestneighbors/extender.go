//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package nearestneighbor

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
)

const (
	DefaultLimit = 10
	DefaultK     = 32
)

type Extender struct {
	searcher contextionary
}

type contextionary interface {
	MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([][]string, [][]float32, error)
}

func (e *Extender) Do(ctx context.Context, in []search.Result, limit *int) ([]search.Result, error) {
	if in == nil {
		return nil, nil
	}

	vectors := make([][]float32, len(in))
	for i, res := range in {
		if res.Vector == nil || len(res.Vector) == 0 {
			return nil, fmt.Errorf("item %d has no vector", i)
		}
		vectors[i] = res.Vector
	}

	words, distances, err := e.searcher.MultiNearestWordsByVector(ctx, vectors, DefaultK, limitOrDefault(limit))
	if err != nil {
		return nil, errors.Wrap(err, "get neighbors for search results")
	}

	if len(words) != len(distances) || len(words) != len(in) {
		return nil, fmt.Errorf("inconsistent results: input=%d words=%d distances=%d", len(in), len(words), len(distances))
	}

	for i, res := range in {
		up := res.UnderscoreProperties
		if up == nil {
			up = &models.UnderscoreProperties{}
		}

		up.NearestNeighbors = &models.NearestNeighbors{
			Neighbors: wordsAndDistancesToNN(words[i], distances[i]),
		}

		in[i].UnderscoreProperties = up
	}

	return in, nil
}

func NewExtender(searcher contextionary) *Extender {
	return &Extender{searcher: searcher}
}

func limitOrDefault(user *int) int {
	if user == nil || *user == 0 {
		return DefaultLimit
	}

	return *user
}

func wordsAndDistancesToNN(words []string, distances []float32) []*models.NearestNeighbor {
	out := make([]*models.NearestNeighbor, len(words))
	for i := range out {
		out[i] = &models.NearestNeighbor{
			Concept:  words[i],
			Distance: distances[i],
		}
	}

	return out
}
