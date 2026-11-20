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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (a *Aggregator) buildHybridKeywordRanking() (*searchparams.KeywordRanking, error) {
	kw := &searchparams.KeywordRanking{
		Type:                 "bm25",
		Query:                a.params.Hybrid.Query,
		MinimumOrTokensMatch: a.params.Hybrid.MinimumOrTokensMatch,
		SearchOperator:       a.params.Hybrid.SearchOperator,
	}

	cl := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if cl == nil {
		return nil, fmt.Errorf("could not find class %s in schema", a.params.ClassName)
	}

	// Named properties are validated by BM25F, which errors when one is not
	// searchable. An empty list is every searchable text property, and a
	// text property with indexSearchable disabled is an error rather than a
	// property that is quietly left out of the keyword leg (#5888).
	if len(a.params.Hybrid.Properties) > 0 {
		kw.Properties = append([]string(nil), a.params.Hybrid.Properties...)
		return kw, nil
	}
	props, err := searchparams.ExpandKeywordProperties(cl)
	if err != nil {
		return nil, err
	}
	kw.Properties = props
	return kw, nil
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
