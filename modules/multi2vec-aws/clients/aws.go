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
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/aws"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/multi2vec-aws/ent"
)

type vectorizer struct {
	client *aws.Client
	logger logrus.FieldLogger
}

func New(awsAccessKey, awsSecret, awsSessionToken string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		client: aws.New(awsAccessKey, awsSecret, awsSessionToken, timeout, logger),
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, texts, images, cfg, aws.GenericIndex)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context,
	texts []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, texts, nil, cfg, aws.GenericRetrieval)
}

func (v *vectorizer) VectorizeImage(ctx context.Context,
	images []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	return v.vectorize(ctx, nil, images, cfg, aws.GenericRetrieval)
}

func (v *vectorizer) vectorize(ctx context.Context,
	texts, images []string, cfg moduletools.ClassConfig,
	embeddingPurpose aws.EmbeddingPurpose,
) (*modulecomponents.VectorizationCLIPResult[[]float32], error) {
	config := ent.NewClassSettings(cfg)
	settings := aws.Settings{
		Model:            config.Model(),
		Region:           config.Region(),
		Dimensions:       config.Dimensions(),
		Service:          aws.Bedrock,
		EmbeddingPurpose: embeddingPurpose,
	}
	return v.client.VectorizeMultiModal(ctx, texts, images, settings)
}
