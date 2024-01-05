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

package nearAudio

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearAudio"

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
	VectorizeAudio(ctx context.Context, audio string) ([]float32, error)
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
	return s.vectorFromNearAudioParam(ctx, params.(*nearAudio.NearAudioParams), className, findVectorFn, cfg)
}

func (s *Searcher) vectorFromNearAudioParam(ctx context.Context,
	params *nearAudio.NearAudioParams, className string, findVectorFn modulecapabilities.FindVectorFn,
	cfg moduletools.ClassConfig,
) ([]float32, error) {
	// find vector for given search query
	vector, err := s.vectorizer.VectorizeAudio(ctx, params.Audio)
	if err != nil {
		return nil, fmt.Errorf("vectorize audio: %w", err)
	}

	return vector, nil
}
