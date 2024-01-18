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

package nearestneighbors

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/pkg/errors"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
)

const (
	DefaultLimit = 10
	DefaultK     = 32
)

type Extender struct {
	searcher contextionary
}

type contextionary interface {
	MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([]*txt2vecmodels.NearestNeighbors, error)
}

func (e *Extender) AdditionalPropertyDefaultValue() interface{} {
	return true
}

func (e *Extender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return e.Multi(ctx, in, limit)
}

func (e *Extender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
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
		up := res.AdditionalProperties
		if up == nil {
			up = models.AdditionalProperties{}
		}

		up["nearestNeighbors"] = removeDollarElements(neighbors[i])
		in[i].AdditionalProperties = up
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

func removeDollarElements(in *txt2vecmodels.NearestNeighbors) *txt2vecmodels.NearestNeighbors {
	neighbors := make([]*txt2vecmodels.NearestNeighbor, len(in.Neighbors))
	i := 0
	for _, elem := range in.Neighbors {
		if elem.Concept[0] == '$' {
			continue
		}

		neighbors[i] = elem
		i++
	}

	return &txt2vecmodels.NearestNeighbors{
		Neighbors: neighbors[:i],
	}
}
