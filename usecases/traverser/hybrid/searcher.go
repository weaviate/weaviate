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

package hybrid

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/autocut"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	uc "github.com/weaviate/weaviate/usecases/schema"
)

const DefaultLimit = 100

type Params struct {
	*searchparams.HybridSearch
	Keyword *searchparams.KeywordRanking
	Class   string
	Autocut int
}

// Result facilitates the pairing of a search result with its internal doc id.
//
// This type is key in generalising hybrid search across different use cases.
// Some use cases require a full search result (Get{} queries) and others need
// only a doc id (Aggregate{}) which the search.Result type does not contain.
// It does now

type Results []*search.Result

// sparseSearchFunc is the signature of a closure which performs sparse search.
// Any package which wishes use hybrid search must provide this. The weights are
// used in calculating the final scores of the result set.
type sparseSearchFunc func() (results []*storobj.Object, weights []float32, err error)

// denseSearchFunc is the signature of a closure which performs dense search.
// A search vector argument is required to pass along to the vector index.
// Any package which wishes use hybrid search must provide this The weights are
// used in calculating the final scores of the result set.
type denseSearchFunc func(searchVector models.Vector) (results []*storobj.Object, weights []float32, err error)

// postProcFunc takes the results of the hybrid search and applies some transformation.
// This is optionally provided, and allows the caller to somehow change the nature of
// the result set. For example, Get{} queries sometimes require resolving references,
// which is implemented by doing the reference resolution within a postProcFunc closure.
type postProcFunc func(hybridResults []*search.Result) (postProcResults []search.Result, err error)

type modulesProvider interface {
	VectorFromInput(ctx context.Context,
		className, input, targetVector string) ([]float32, error)
	MultiVectorFromInput(ctx context.Context,
		className, input, targetVector string) ([][]float32, error)
	IsTargetVectorMultiVector(className, targetVector string) (bool, error)
}

type targetVectorParamHelper interface {
	GetTargetVectorOrDefault(sch schema.Schema, className string, targetVector []string) ([]string, error)
}

// Search executes sparse and dense searches and combines the result sets using Reciprocal Rank Fusion
func Search(ctx context.Context, params *Params, logger logrus.FieldLogger, sparseSearch sparseSearchFunc, denseSearch denseSearchFunc, postProc postProcFunc, modules modulesProvider, schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper) ([]*search.Result, error) {
	var (
		found   [][]*search.Result
		weights []float64
		names   []string
	)

	alpha := params.Alpha
	var belowCutoffSet map[strfmt.UUID]struct{}
	if alpha < 1 {
		if params.Query != "" {
			res, err := processSparseSearch(sparseSearch())
			if err != nil {
				return nil, err
			}

			found = append(found, res)
			weights = append(weights, 1-alpha)
			names = append(names, "keyword")
		}
	}

	if alpha > 0 {
		res, err := processDenseSearch(ctx, denseSearch, params, modules, schemaGetter, targetVectorParamHelper)
		if err != nil {
			return nil, err
		}
		if params.WithDistance {
			belowCutoffSet = map[strfmt.UUID]struct{}{}
			// index starts with 0, use one less so we do not get any results in case nothing is above the limit
			maxFound := -1
			for i := range res {
				if res[i].Dist <= params.HybridSearch.Distance {
					belowCutoffSet[res[i].ID] = struct{}{}
					maxFound = i
				} else {
					break
				}
			}
			// sorted by distance, so just remove everything after the first entry we found
			res = res[:maxFound+1]
		}

		found = append(found, res)
		weights = append(weights, alpha)
		names = append(names, "vector")
	}

	// remove results with a vector distance above the cutoff from the BM25 results
	if alpha < 1 && belowCutoffSet != nil {
		newResults := make([]*search.Result, 0, len(found[0]))
		for i := range found[0] {
			if _, ok := belowCutoffSet[found[0][i].ID]; ok {
				newResults = append(newResults, found[0][i])
			}
		}
		found[0] = newResults
	}

	if len(weights) != len(found) {
		return nil, fmt.Errorf("length of weights and results do not match for hybrid search %v vs. %v", len(weights), len(found))
	}

	var fused []*search.Result
	if params.FusionAlgorithm == common_filters.HybridRankedFusion {
		fused = FusionRanked(weights, found, names)
	} else if params.FusionAlgorithm == common_filters.HybridRelativeScoreFusion {
		fused = FusionRelativeScore(weights, found, names, true)
	} else {
		return nil, fmt.Errorf("unknown ranking algorithm %v for hybrid search", params.FusionAlgorithm)
	}

	if postProc != nil {
		sr, err := postProc(fused)
		if err != nil {
			return nil, fmt.Errorf("hybrid search post-processing: %w", err)
		}
		newResults := make([]*search.Result, len(sr))
		for i := range sr {
			newResults[i] = &sr[i]
		}
		fused = newResults
	}
	if params.Autocut > 0 {
		scores := make([]float32, len(fused))
		for i := range fused {
			scores[i] = fused[i].Score
		}
		cutOff := autocut.Autocut(scores, params.Autocut)
		fused = fused[:cutOff]
	}
	return fused, nil
}

// Search combines the result sets using Reciprocal Rank Fusion or Relative Score Fusion
func HybridCombiner(ctx context.Context, params *Params, resultSet [][]*search.Result, weights []float64, names []string, logger logrus.FieldLogger, postProc postProcFunc) ([]*search.Result, error) {
	if params.Vector != nil && params.NearVectorParams != nil {
		return nil, fmt.Errorf("hybrid search: cannot have both vector and nearVectorParams")
	}
	if params.Vector != nil && params.NearTextParams != nil {
		return nil, fmt.Errorf("hybrid search: cannot have both vector and nearTextParams")
	}
	if params.NearTextParams != nil && params.NearVectorParams != nil {
		return nil, fmt.Errorf("hybrid search: cannot have both nearTextParams and nearVectorParams")
	}

	if len(weights) != len(resultSet) {
		return nil, fmt.Errorf("length of weights and results do not match for hybrid search %v vs. %v", len(weights), len(resultSet))
	}
	if len(weights) != len(names) {
		return nil, fmt.Errorf("length of weights and names do not match for hybrid search %v vs. %v", len(weights), len(names))
	}

	var fused []*search.Result
	if params.FusionAlgorithm == common_filters.HybridRankedFusion {
		fused = FusionRanked(weights, resultSet, names)
	} else if params.FusionAlgorithm == common_filters.HybridRelativeScoreFusion {
		fused = FusionRelativeScore(weights, resultSet, names, true)
	} else {
		return nil, fmt.Errorf("unknown ranking algorithm %v for hybrid search", params.FusionAlgorithm)
	}

	if postProc != nil {
		sr, err := postProc(fused)
		if err != nil {
			return nil, fmt.Errorf("hybrid search post-processing: %w", err)
		}
		newResults := make([]*search.Result, len(sr))
		for i := range sr {
			newResults[i] = &sr[i]
		}
		fused = newResults
	}

	if params.Autocut > 0 {
		scores := make([]float32, len(fused))
		for i := range fused {
			scores[i] = fused[i].Score
		}
		cutOff := autocut.Autocut(scores, params.Autocut)
		fused = fused[:cutOff]
	}
	return fused, nil
}

func processSparseSearch(results []*storobj.Object, scores []float32, err error) ([]*search.Result, error) {
	if err != nil {
		return nil, fmt.Errorf("sparse search: %w", err)
	}

	out := make([]*search.Result, len(results))
	for i, obj := range results {
		sr := obj.SearchResultWithScore(additional.Properties{}, scores[i])
		sr.SecondarySortValue = sr.Score
		out[i] = &sr
	}
	return out, nil
}

func processDenseSearch(ctx context.Context,
	denseSearch denseSearchFunc, params *Params, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) ([]*search.Result, error) {
	query := params.Query
	vector := params.Vector
	if params.HybridSearch.NearTextParams != nil {
		params.NearTextParams = params.HybridSearch.NearTextParams
		query = params.HybridSearch.NearTextParams.Values[0]
	}

	if params.HybridSearch.NearVectorParams == nil {
		var err error
		vector, err = decideSearchVector(ctx, params.Class, query, params.TargetVectors, vector, modules, schemaGetter, targetVectorParamHelper)
		if err != nil {
			return nil, err
		}
	} else {
		vector = params.HybridSearch.NearVectorParams.Vectors[0]
	}

	res, dists, err := denseSearch(vector)
	if err != nil {
		return nil, fmt.Errorf("dense search: %w", err)
	}

	out := make([]*search.Result, len(res))
	for i, obj := range res {
		sr := obj.SearchResultWithDist(additional.Properties{}, dists[i])
		sr.SecondarySortValue = 1 - sr.Dist
		out[i] = &sr
	}
	return out, nil
}

func decideSearchVector(ctx context.Context,
	class, query string, targetVectors []string, vector models.Vector, modules modulesProvider,
	schemaGetter uc.SchemaGetter, targetVectorParamHelper targetVectorParamHelper,
) (models.Vector, error) {
	isVectorEmpty, err := dto.IsVectorEmpty(vector)
	if err != nil {
		return nil, fmt.Errorf("decide search vector: is vector empty: %w", err)
	}
	if !isVectorEmpty {
		return vector, nil
	} else {
		if modules != nil && schemaGetter != nil && targetVectorParamHelper != nil {
			targetVectors, err := targetVectorParamHelper.GetTargetVectorOrDefault(schemaGetter.GetSchemaSkipAuth(),
				class, targetVectors)
			targetVector := getTargetVector(targetVectors)
			if err != nil {
				return nil, err
			}
			vector, err := vectorFromModuleInput(ctx, class, query, targetVector, modules)
			if err != nil {
				return nil, err
			}
			return vector, nil
		} else {
			return nil, fmt.Errorf("no vector or modules provided for hybrid search")
		}
	}
}

func vectorFromModuleInput(ctx context.Context, class, input, targetVector string, modules modulesProvider) (models.Vector, error) {
	isMultiVector, err := modules.IsTargetVectorMultiVector(class, targetVector)
	if err != nil {
		return nil, fmt.Errorf("get vector input from modules provider: is target vector multi vector: %w", err)
	}
	if isMultiVector {
		vector, err := modules.MultiVectorFromInput(ctx, class, input, targetVector)
		if err != nil {
			return nil, fmt.Errorf("get multi vector input from modules provider: %w", err)
		}
		return vector, nil
	}
	vector, err := modules.VectorFromInput(ctx, class, input, targetVector)
	if err != nil {
		return nil, fmt.Errorf("get vector input from modules provider: %w", err)
	}
	return vector, nil
}

func getTargetVector(targetVectors []string) string {
	if len(targetVectors) == 1 {
		return targetVectors[0]
	}
	return ""
}
