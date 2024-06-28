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

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

type client struct {
	logger logrus.FieldLogger
}

func New(logger logrus.FieldLogger) *client {
	return &client{
		logger: logger,
	}
}

func (c *client) Rank(ctx context.Context, query string, documents []string,
	cfg moduletools.ClassConfig,
) (*ent.RankResult, error) {
	documentScores := make([]ent.DocumentScore, 0, len(documents))
	for _, doc := range documents {
		documentScores = append(documentScores, ent.DocumentScore{
			Document: doc,
			Score:    float64(len(doc)),
		})
	}

	return &ent.RankResult{
		Query: query, DocumentScores: documentScores,
	}, nil
}
