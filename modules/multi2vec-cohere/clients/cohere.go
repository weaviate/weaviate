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

package clients

import (
	"context"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/cohere"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/multi2vec-cohere/ent"
)

type vectorizer struct {
	client *cohere.Client
	logger logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		client: cohere.New(apiKey, timeout, logger),
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult, error) {
	return v.vectorize(ctx, texts, images, cfg)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context,
	input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult, error) {
	return v.vectorize(ctx, input, nil, cfg)
}

func (v *vectorizer) vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult, error) {
	var textVectors [][]float32
	var imageVectors [][]float32
	settings := ent.NewClassSettings(cfg)
	if len(texts) > 0 {
		textEmbeddings, err := v.client.Vectorize(ctx, texts, cohere.Settings{
			Model:     settings.Model(),
			Truncate:  settings.Truncate(),
			BaseURL:   settings.BaseURL(),
			InputType: cohere.SearchDocument,
		})
		if err != nil {
			return nil, err
		}
		textVectors = textEmbeddings.Vector
	}
	if len(images) > 0 {
		imageEmbeddings, err := v.client.Vectorize(ctx, images, cohere.Settings{
			Model:     settings.Model(),
			BaseURL:   settings.BaseURL(),
			InputType: cohere.Image,
		})
		if err != nil {
			return nil, err
		}
		imageVectors = imageEmbeddings.Vector
	}
	return &modulecomponents.VectorizationCLIPResult{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}, nil
}
