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

package nearImage

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type Searcher[T dto.Embedding] struct {
	vectorizer imgVectorizer[T]
}

func NewSearcher[T dto.Embedding](vectorizer imgVectorizer[T]) *Searcher[T] {
	return &Searcher[T]{vectorizer}
}

type imgVectorizer[T dto.Embedding] interface {
	VectorizeImage(ctx context.Context,
		id, image string, cfg moduletools.ClassConfig) (T, error)
}

func (s *Searcher[T]) VectorSearches() map[string]modulecapabilities.VectorForParams[T] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[T]{}
	vectorSearches["nearImage"] = &vectorForParams[T]{s.vectorizer}
	return vectorSearches
}

type vectorForParams[T dto.Embedding] struct {
	vectorizer imgVectorizer[T]
}

func (v *vectorForParams[T]) VectorForParams(ctx context.Context, params interface{}, className string,
	findVectorFn modulecapabilities.FindVectorFn[T],
	cfg moduletools.ClassConfig,
) (T, error) {
	searchID := fmt.Sprintf("search_%v", time.Now().UnixNano())
	vector, err := v.vectorizer.VectorizeImage(ctx, searchID, params.(*NearImageParams).Image, cfg)
	if err != nil {
		return nil, errors.Errorf("vectorize image: %v", err)
	}
	return vector, nil
}
