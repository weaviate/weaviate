//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package nearImu

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type Searcher[T dto.Embedding] struct {
	vectorForParams modulecapabilities.VectorForParams[T]
}

func NewSearcher[T dto.Embedding](vectorizer bindVectorizer[T]) *Searcher[T] {
	return &Searcher[T]{func(ctx context.Context, params any, className string,
		findVectorFn modulecapabilities.FindVectorFn[T],
		cfg moduletools.ClassConfig,
	) (T, error) {
		// find vector for given search query
		vector, err := vectorizer.VectorizeIMU(ctx, params.(*NearIMUParams).IMU, cfg)
		if err != nil {
			return nil, errors.Errorf("vectorize imu: %v", err)
		}
		return vector, nil
	}}
}

type bindVectorizer[T dto.Embedding] interface {
	VectorizeIMU(ctx context.Context, video string, cfg moduletools.ClassConfig) (T, error)
}

func (s *Searcher[T]) VectorSearches() map[string]modulecapabilities.VectorForParams[T] {
	vectorSearches := map[string]modulecapabilities.VectorForParams[T]{}
	vectorSearches["nearIMU"] = s.vectorForParams
	return vectorSearches
}
