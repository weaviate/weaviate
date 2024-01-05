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

package nearimu

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
	VectorizeIMU(ctx context.Context, video string) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearIMU"] = s.vectorForNearIMUParam
	return vectorSearches
}

func (s *Searcher) vectorForNearIMUParam(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearIMUParam(ctx, params.(*NearIMUParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearIMUParam(ctx context.Context,
	params *NearIMUParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	vector, err := s.vectorizer.VectorizeIMU(ctx, params.IMU)
	if err != nil {
		return nil, errors.Errorf("vectorize imu: %v", err)
	}

	return vector, nil
}
