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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/modules/text2vec-aws/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/aws"
)

type operationType string

var (
	vectorizeObject operationType = "vectorize_object"
	vectorizeQuery  operationType = "vectorize_query"
)

type awsClient struct {
	client *aws.Client
	logger logrus.FieldLogger
}

func New(awsAccessKey, awsSecret, awsSessionToken string, timeout time.Duration, logger logrus.FieldLogger) *awsClient {
	return &awsClient{
		client: aws.New(awsAccessKey, awsSecret, awsSessionToken, timeout, logger),
		logger: logger,
	}
}

func (v *awsClient) Vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	return v.vectorize(ctx, input, config, aws.Document)
}

func (v *awsClient) VectorizeQuery(ctx context.Context, input []string,
	config ent.VectorizationConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	return v.vectorize(ctx, input, config, aws.Query)
}

func (v *awsClient) vectorize(ctx context.Context, input []string,
	config ent.VectorizationConfig,
	operationType aws.OperationType,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	return v.client.Vectorize(ctx, input, aws.Settings{
		Model:         config.Model,
		Region:        config.Region,
		Service:       aws.Service(config.Service),
		Endpoint:      config.Endpoint,
		OperationType: operationType,
	})
}
