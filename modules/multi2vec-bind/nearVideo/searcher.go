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

package nearVideo

import (
	"context"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearVideo"

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
	VectorizeVideo(ctx context.Context, video string) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearVideo"] = s.vectorForNearVideoParam
	return vectorSearches
}

func (s *Searcher) vectorForNearVideoParam(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearVideoParam(ctx, params.(*nearVideo.NearVideoParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearVideoParam(ctx context.Context,
	params *nearVideo.NearVideoParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	vector, err := s.vectorizer.VectorizeVideo(ctx, params.Video)
	if err != nil {
		return nil, errors.Errorf("vectorize video: %v", err)
	}

	return vector, nil
}
