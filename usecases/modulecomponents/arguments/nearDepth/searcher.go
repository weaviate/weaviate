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

package nearDepth

import (
	"context"

	"github.com/pkg/errors"
	"github.com/liutizhong/weaviate/entities/modulecapabilities"
	"github.com/liutizhong/weaviate/entities/moduletools"
	"github.com/liutizhong/weaviate/entities/types"
)

type Searcher[T types.Embedding] struct {
	vectorizer bindVectorizer[T]
}

func NewSearcher[T types.Embedding](vectorizer bindVectorizer[T]) *Searcher[T] {
	return &Searcher[T]{vectorizer}
}

type bindVectorizer[T types.Embedding] interface {
	VectorizeDepth(ctx context.Context, thermal string, cfg moduletools.ClassConfig) (T, error)
}

func (s *Searcher[T]) VectorSearches() map[string]modulecapabilities.VectorForParams[T] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[T]{}
	vectorSearches["nearDepth"] = &vectorForParams[T]{s.vectorizer}
	return vectorSearches
}

type vectorForParams[T types.Embedding] struct {
	vectorizer bindVectorizer[T]
}

func (v *vectorForParams[T]) VectorForParams(ctx context.Context, params interface{}, className string,
	findVectorFn modulecapabilities.FindVectorFn[T],
	cfg moduletools.ClassConfig,
) (T, error) {
	vector, err := v.vectorizer.VectorizeDepth(ctx, params.(*NearDepthParams).Depth, cfg)
	if err != nil {
		return nil, errors.Errorf("vectorize thermal: %v", err)
	}
	return vector, nil
}
