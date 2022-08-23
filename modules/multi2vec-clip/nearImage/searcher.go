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

package nearImage

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
)

type Searcher struct {
	vectorizer imgVectorizer
}

func NewSearcher(vectorizer imgVectorizer) *Searcher {
	return &Searcher{vectorizer}
}

type imgVectorizer interface {
	VectorizeImage(ctx context.Context, image string) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearImage"] = s.vectorForNearImageParam
	return vectorSearches
}

func (s *Searcher) vectorForNearImageParam(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearImageParam(ctx, params.(*NearImageParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearImageParam(ctx context.Context,
	params *NearImageParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	vector, err := s.vectorizer.VectorizeImage(ctx, params.Image)
	if err != nil {
		return nil, errors.Errorf("vectorize image: %v", err)
	}

	return vector, nil
}
