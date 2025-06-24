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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/modulecomponents/clients/transformers"
)

type vectorizer struct {
	originPassage string
	originQuery   string
	client        *transformers.Client
	urlBuilder    *transformers.URLBuilder
	logger        logrus.FieldLogger
}

func New(originPassage, originQuery string, timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	urlBuilder := transformers.NewURLBuilder(originPassage, originQuery)
	return &vectorizer{
		originPassage: originPassage,
		originQuery:   originQuery,
		urlBuilder:    urlBuilder,
		client:        transformers.New(urlBuilder, timeout, logger),
		logger:        logger,
	}
}

func (v *vectorizer) VectorizeObject(ctx context.Context, input string,
	config transformers.VectorizationConfig,
) (*transformers.VectorizationResult, error) {
	return v.client.VectorizeObject(ctx, input, config)
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input string,
	config transformers.VectorizationConfig,
) (*transformers.VectorizationResult, error) {
	return v.client.VectorizeQuery(ctx, input, config)
}
