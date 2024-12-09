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
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/voyageai"
)

type voyageaiUrlBuilder struct {
	origin   string
	pathMask string
}

func newVoyageAIUrlBuilder() *voyageaiUrlBuilder {
	return &voyageaiUrlBuilder{
		origin:   ent.DefaultBaseURL,
		pathMask: "/multimodalembeddings",
	}
}

func (c *voyageaiUrlBuilder) URL(baseURL string) string {
	if baseURL != "" {
		return fmt.Sprintf("%s%s", baseURL, c.pathMask)
	}
	return fmt.Sprintf("%s%s", c.origin, c.pathMask)
}

type vectorizer struct {
	client *voyageai.Client
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		client: voyageai.New(apiKey, timeout, newVoyageAIUrlBuilder(), logger),
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	settings := ent.NewClassSettings(cfg)
	return v.client.VectorizeMultiModal(ctx, texts, images, voyageai.Settings{
		BaseURL:   settings.BaseURL(),
		Model:     settings.Model(),
		Truncate:  settings.Truncate(),
		InputType: voyageai.Document,
	})
}

func (v *vectorizer) VectorizeQuery(ctx context.Context,
	input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	settings := ent.NewClassSettings(cfg)
	return v.client.VectorizeMultiModal(ctx, input, nil, voyageai.Settings{
		BaseURL:   settings.BaseURL(),
		Model:     settings.Model(),
		Truncate:  settings.Truncate(),
		InputType: voyageai.Query,
	})
}

func (v *vectorizer) VectorizeImageQuery(ctx context.Context,
	images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	settings := ent.NewClassSettings(cfg)
	return v.client.VectorizeMultiModal(ctx, nil, images, voyageai.Settings{
		BaseURL:   settings.BaseURL(),
		Model:     settings.Model(),
		Truncate:  settings.Truncate(),
		InputType: voyageai.Query,
	})
}
