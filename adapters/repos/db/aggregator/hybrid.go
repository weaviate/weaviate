//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (a *Aggregator) buildHybridKeywordRanking() (*searchparams.KeywordRanking, error) {
	cl := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if cl == nil {
		return nil, fmt.Errorf("could not find class %s in schema", a.params.ClassName)
	}

	return HybridKeywordRanking(a.params.Hybrid, cl), nil
}

// HybridKeywordRanking builds the BM25 leg of a hybrid aggregation over the
// text properties of class. It is shared with the index, which runs the same
// search across all shards when it resolves a hybrid aggregation's objects
// globally, so both must rank identically.
func HybridKeywordRanking(h *searchparams.HybridSearch, class *models.Class) *searchparams.KeywordRanking {
	kw := &searchparams.KeywordRanking{
		Type:                 "bm25",
		Query:                h.Query,
		MinimumOrTokensMatch: h.MinimumOrTokensMatch,
		SearchOperator:       h.SearchOperator,
	}

	for _, v := range class.Properties {
		if v.DataType[0] == "text" || v.DataType[0] == "string" { // TODO: Also the array types?
			kw.Properties = append(kw.Properties, v.Name)
		}
	}

	return kw
}

// CapFusedResults keeps the limit best results of a hybrid search. Fusion
// returns the union of the BM25 and vector results, sorted best first, so a
// mixed search (0 < alpha < 1) can return up to twice the limit; a hybrid
// aggregation must cover at most limit objects, the top limit of the fused
// ranking, like a hybrid Get with that limit.
func CapFusedResults(res []search.Result, limit int) []search.Result {
	if limit >= 0 && len(res) > limit {
		return res[:limit]
	}
	return res
}

// setDefaultObjectLimit fills in the limit a hybrid request may omit. Both legs
// read it, and either can run alone depending on alpha, so it has to be set
// before the search rather than inside one leg.
func (a *Aggregator) setDefaultObjectLimit() {
	if a.params.ObjectLimit == nil {
		limit := int(a.defaultLimit)
		a.params.ObjectLimit = &limit
	}
}

func (a *Aggregator) bm25Objects(ctx context.Context, kw *searchparams.KeywordRanking, allowList helpers.AllowList) ([]*storobj.Object, []float32, error) {
	class := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if class == nil {
		return nil, nil, fmt.Errorf("bm25 objects: could not find class %s in schema", a.params.ClassName)
	}
	cfg := inverted.ConfigFromModel(class.InvertedIndexConfig)

	kw.ChooseSearchableProperties(class)

	objs, dists, err := inverted.NewBM25Searcher(cfg.BM25, a.store, a.getSchema.ReadOnlyClass,
		a.classSearcher, a.stopwordProvider,
		a.GetPropertyLengthTracker(), a.logger, a.shardVersion,
	).WithTokenizationResolver(a.tokResolver).
		WithSearchableBucketPinningResolver(a.bucketPinResolver).
		BM25F(ctx, allowList, a.params.ClassName, *a.params.ObjectLimit, *kw, additional.Properties{})
	if err != nil {
		return nil, nil, fmt.Errorf("bm25 objects: %w", err)
	}
	return objs, dists, nil
}
