//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/propertyspecific"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

func (a *Aggregator) buildHybridKeywordRanking() (*searchparams.KeywordRanking, error) {
	kw := &searchparams.KeywordRanking{
		Type:  "bm25",
		Query: a.params.Hybrid.Query,
	}

	cl, err := schema.GetClassByName(
		a.getSchema.GetSchemaSkipAuth().Objects,
		a.params.ClassName.String())
	if err != nil {
		return nil, err
	}

	for _, v := range cl.Properties {
		if v.DataType[0] == "text" || v.DataType[0] == "string" { // TODO: Also the array types?
			kw.Properties = append(kw.Properties, v.Name)
		}
	}

	return kw, nil
}

func (a *Aggregator) bm25Objects(ctx context.Context, kw *searchparams.KeywordRanking) ([]*storobj.Object, []float32, error) {
	var (
		s     = a.getSchema.GetSchemaSkipAuth()
		class = s.GetClass(a.params.ClassName)
		cfg   = inverted.ConfigFromModel(class.InvertedIndexConfig)
	)

	objs, dists, err := inverted.NewBM25Searcher(cfg.BM25, a.store, s,
		a.invertedRowCache, propertyspecific.Indices{}, a.classSearcher,
		nil, a.propLengths, a.logger, a.shardVersion,
	).Objects(ctx, *a.params.ObjectLimit, kw, a.params.Filters,
		nil, additional.Properties{Vector: true}, a.params.ClassName)
	if err != nil {
		return nil, nil, fmt.Errorf("bm25 objects: %w", err)
	}
	return objs, dists, nil
}
