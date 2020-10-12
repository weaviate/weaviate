//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package nearestneighbors

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
	MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*models.NearestNeighbors, error)
}

func (e *Extender) Single(ctx context.Context, in *search.Result, limit *int) (*search.Result, error) {
	if in == nil {
		return nil, nil
	}

	multiRes, err := e.Multi(ctx, []search.Result{*in}, limit) // safe to deref, as we did a nil check before
	if err != nil {
		return nil, err
	}

	return &multiRes[0], nil
}

func (e *Extender) Multi(ctx context.Context, in []search.Result, limit *int) ([]search.Result, error) {
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

	neighbors, err := e.searcher.MultiNearestWordsByVector(ctx, vectors, DefaultK, limitOrDefault(limit))
	if err != nil {
		return nil, errors.Wrap(err, "get neighbors for search results")
	}

	if len(neighbors) != len(in) {
		return nil, fmt.Errorf("inconsistent results: input=%d neighbors=%d", len(in), len(neighbors))
	}

	for i, res := range in {
		up := res.UnderscoreProperties
		if up == nil {
			up = &models.UnderscoreProperties{}
		}

		up.NearestNeighbors = removeDollarElements(neighbors[i])
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

func removeDollarElements(in *models.NearestNeighbors) *models.NearestNeighbors {
	neighbors := make([]*models.NearestNeighbor, len(in.Neighbors))
	i := 0
	for _, elem := range in.Neighbors {
		if elem.Concept[0] == '$' {
			continue
		}

		neighbors[i] = elem
		i++
	}

	return &models.NearestNeighbors{
		Neighbors: neighbors[:i],
	}
}
