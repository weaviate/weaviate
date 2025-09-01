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
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/openai"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-morph/ent"
)

type client struct {
	client *openai.Client
	logger logrus.FieldLogger
}

func New(openAIApiKey string, timeout time.Duration, logger logrus.FieldLogger) *client {
	return &client{
		client: openai.New(openAIApiKey, "", "", timeout, logger),
		logger: logger,
	}
}

func (v *client) Vectorize(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	config := v.getSettings(cfg, "document")
	return v.client.Vectorize(ctx, input, config)
}

func (v *client) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	config := v.getSettings(cfg, "query")
	return v.client.VectorizeQuery(ctx, input, config)
}

func (v *client) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	config := v.getSettings(cfg, "document")
	return v.client.GetApiKeyHash(ctx, config)
}

func (v *client) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	config := v.getSettings(cfg, "document")
	return v.client.GetVectorizerRateLimit(ctx, config)
}

func (v *client) getSettings(cfg moduletools.ClassConfig, action string) openai.Settings {
	settings := ent.NewClassSettings(cfg)
	return openai.Settings{
		Type:                 "text",
		Model:                settings.Model(),
		ModelVersion:         settings.Model(),
		ResourceName:         "", // Not used by Morph
		DeploymentID:         "", // Not used by Morph
		BaseURL:              settings.BaseURL(),
		IsAzure:              false, // Morph doesn't support Azure
		IsThirdPartyProvider: true,  // Morph is always third-party
		ApiVersion:           "",    // Not used by Morph
		Dimensions:           nil,
		ModelString:          settings.Model(),
	}
}
