//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package nearVideo

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type Searcher struct {
	vectorizer imgVectorizer
}

func NewSearcher(vectorizer imgVectorizer) *Searcher {
	return &Searcher{vectorizer}
}

type imgVectorizer interface {
	VectorizeImage(ctx context.Context, image string) ([]float32, error)
	Vectorize(ctx context.Context,
		texts, images, audio, video, imu, thermal, depth []string,
	) ([]float32, error)
}

func (s *Searcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearAudio"] = s.vectorForNearAudioParam
	return vectorSearches
}

func (s *Searcher) vectorForNearAudioParam(ctx context.Context, params interface{},
	className string,
	findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	return s.vectorFromNearAudioParam(ctx, params.(*NearVideoParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearAudioParam(ctx context.Context,
	params *NearVideoParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	video := []string{params.Video}
	vector, err := s.vectorizer.Vectorize(ctx, []string{}, []string{}, []string{}, video, []string{}, []string{}, []string{})
	if err != nil {
		return nil, errors.Errorf("vectorize audio: %v", err)
	}

	return vector, nil
}
