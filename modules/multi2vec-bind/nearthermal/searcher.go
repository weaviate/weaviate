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

package nearthermal

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type Searcher struct {
	vectorizer bindVectorizer
}

func NewSearcher(vectorizer bindVectorizer) *Searcher {
	return &Searcher{vectorizer}
}

type bindVectorizer interface {
	VectorizeThermal(ctx context.Context, thermal string) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearThermal"] = s.vectorForNearThermalParam
	return vectorSearches
}

func (s *Searcher) vectorForNearThermalParam(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearThermalParam(ctx, params.(*NearThermalParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearThermalParam(ctx context.Context,
	params *NearThermalParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	vector, err := s.vectorizer.VectorizeThermal(ctx, params.Thermal)
	if err != nil {
		return nil, errors.Errorf("vectorize thermal: %v", err)
	}

	return vector, nil
}
