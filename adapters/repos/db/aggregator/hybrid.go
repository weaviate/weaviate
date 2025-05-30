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

package aggregator

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/additional"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
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

	for _, v := range cl.Properties {
		if v.DataType[0] == "text" || v.DataType[0] == "string" { // TODO: Also the array types?
			kw.Properties = append(kw.Properties, v.Name)
		}
	}

	return kw, nil
}

func (a *Aggregator) bm25Objects(ctx context.Context, kw *searchparams.KeywordRanking) ([]*storobj.Object, []float32, error) {
	class := a.getSchema.ReadOnlyClass(a.params.ClassName.String())
	if class == nil {
		return nil, nil, fmt.Errorf("bm25 objects: could not find class %s in schema", a.params.ClassName)
	}
	cfg := inverted.ConfigFromModel(class.InvertedIndexConfig)

	kw.ChooseSearchableProperties(class)

	objs, dists, err := inverted.NewBM25Searcher(cfg.BM25, a.store, a.getSchema.ReadOnlyClass,
		propertyspecific.Indices{}, a.classSearcher,
		a.GetPropertyLengthTracker(), a.logger, a.shardVersion,
	).BM25F(ctx, nil, a.params.ClassName, *a.params.ObjectLimit, *kw, additional.Properties{})
	if err != nil {
		return nil, nil, fmt.Errorf("bm25 objects: %w", err)
	}
	return objs, dists, nil
}
