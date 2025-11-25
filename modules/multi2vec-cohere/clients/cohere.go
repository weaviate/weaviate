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

package clients

import (
	"context"
	"fmt"
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
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, texts, images, cfg)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context,
	input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, input, nil, cfg)
}

func (v *vectorizer) VectorizeImages(ctx context.Context,
	images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, nil, images, cfg)
}

func (v *vectorizer) vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	var textVectors [][]float32
	var imageVectors [][]float32
	settings := ent.NewClassSettings(cfg)
	if len(texts) > 0 {
		textEmbeddings, err := v.client.Vectorize(ctx, texts, cohere.Settings{
			Model:      settings.Model(),
			Truncate:   settings.Truncate(),
			BaseURL:    settings.BaseURL(),
			InputType:  cohere.SearchDocument,
			Dimensions: settings.Dimensions(),
		})
		if err != nil {
			return nil, err
		}
		textVectors = textEmbeddings.Vector
	}
	if len(images) > 0 {
		// Cohere API allows to send only 1 image per request, we need to loop over images
		// one by one in order to perform the request.
		// https://docs.cohere.com/reference/embed#request.body.images
		for i := range images {
			imageEmbeddings, err := v.client.Vectorize(ctx, []string{images[i]}, cohere.Settings{
				Model:      settings.Model(),
				BaseURL:    settings.BaseURL(),
				InputType:  cohere.Image,
				Dimensions: settings.Dimensions(),
			})
			if err != nil {
				return nil, err
			}
			if len(imageEmbeddings.Vector) != 1 {
				return nil, fmt.Errorf("generated more than one embedding for image, got: %v", len(imageEmbeddings.Vector))
			}
			imageVectors = append(imageVectors, imageEmbeddings.Vector[0])
		}
	}
	return &modulecomponents.VectorizationCLIPResult[[]float32]{
		TextVectors:  textVectors,
		ImageVectors: imageVectors,
	}, nil
}
