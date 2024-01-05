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

package projector

import (
	"context"
	"fmt"
	"time"

	"github.com/danaugrs/go-tsne/tsne"
	"github.com/pkg/errors"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
	"gonum.org/v1/gonum/mat"
)

func New() *FeatureProjector {
	return &FeatureProjector{
		fixedSeed: time.Now().UnixNano(),
	}
}

type FeatureProjector struct {
	fixedSeed int64
}

func (f *FeatureProjector) AdditionalPropertyDefaultValue() interface{} {
	return &Params{}
}

func (f *FeatureProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	if parameters, ok := params.(*Params); ok {
		return f.Reduce(in, parameters)
	}
	return nil, errors.New("unknown params")
}

func (f *FeatureProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return parseFeatureProjectionArguments(param)
}

func (f *FeatureProjector) Reduce(in []search.Result, params *Params) ([]search.Result, error) {
	if len(in) == 0 {
		return nil, nil
	}

	if params == nil {
		return nil, fmt.Errorf("no params provided")
	}

	dims := len(in[0].Vector)

	if err := params.SetDefaultsAndValidate(len(in), dims); err != nil {
		return nil, errors.Wrap(err, "invalid params")
	}

	matrix, err := f.vectorsToMatrix(in, dims, params)
	if err != nil {
		return nil, err
	}
	t := tsne.NewTSNE(*params.Dimensions, float64(*params.Perplexity),
		float64(*params.LearningRate), *params.Iterations, false)
	t.EmbedData(matrix, nil)
	rows, cols := t.Y.Dims()
	if rows != len(in) {
		return nil, fmt.Errorf("incorrect matrix dimensions after t-SNE len %d != %d", len(in), rows)
	}

	for i := 0; i < rows; i++ {
		vector := make([]float32, cols)
		for j := range vector {
			vector[j] = float32(t.Y.At(i, j))
		}
		up := in[i].AdditionalProperties
		if up == nil {
			up = models.AdditionalProperties{}
		}

		up["featureProjection"] = &txt2vecmodels.FeatureProjection{
			Vector: vector,
		}

		in[i].AdditionalProperties = up
	}

	return in, nil
}

func (f *FeatureProjector) vectorsToMatrix(in []search.Result, dims int, params *Params) (*mat.Dense, error) {
	items := len(in)
	var neighbors []*txt2vecmodels.NearestNeighbor
	if params.IncludeNeighbors {
		neighbors = f.extractNeighborsAndRemoveDuplicates(in)
		items += len(neighbors)
	}

	// concat all vectors to build gonum dense matrix
	mergedVectors := make([]float64, items*dims)
	for i, obj := range in {
		if l := len(obj.Vector); l != dims {
			return nil, fmt.Errorf("inconsistent vector lengths found: %d and %d", dims, l)
		}

		for j, dim := range obj.Vector {
			mergedVectors[i*dims+j] = float64(dim)
		}
	}

	withoutNeighbors := len(in) * dims
	for i, neighbor := range neighbors {
		neighborVector := neighbor.Vector
		if l := len(neighborVector); l != dims {
			return nil, fmt.Errorf("inconsistent vector lengths found: %d and %d", dims, l)
		}

		for j, dim := range neighborVector {
			mergedVectors[withoutNeighbors-1+i*dims+j] = float64(dim)
		}
	}

	return mat.NewDense(len(in), dims, mergedVectors), nil
}

func (f *FeatureProjector) extractNeighborsAndRemoveDuplicates(in []search.Result) []*txt2vecmodels.NearestNeighbor {
	var out []*txt2vecmodels.NearestNeighbor

	for _, obj := range in {
		if obj.AdditionalProperties == nil || obj.AdditionalProperties["nearestNeighbors"] == nil {
			continue
		}

		if neighbors, ok := obj.AdditionalProperties["nearestNeighbors"]; ok {
			if nearestNeighbors, ok := neighbors.(*txt2vecmodels.NearestNeighbors); ok {
				out = append(out, nearestNeighbors.Neighbors...)
			}
		}
	}

	return f.removeDuplicateNeighbors(out)
}

func (f *FeatureProjector) removeDuplicateNeighbors(in []*txt2vecmodels.NearestNeighbor) []*txt2vecmodels.NearestNeighbor {
	seen := map[string]struct{}{}
	out := make([]*txt2vecmodels.NearestNeighbor, len(in))

	i := 0
	for _, candidate := range in {
		if _, ok := seen[candidate.Concept]; ok {
			continue
		}

		out[i] = candidate
		i++
		seen[candidate.Concept] = struct{}{}
	}

	return out[:i]
}
