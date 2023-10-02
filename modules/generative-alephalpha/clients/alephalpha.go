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

package clients

import (
	"context"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

type alephalpha struct {
	alephAlphaAPIKey string
	httpClient	  *http.Client
	logger 	  logrus.FieldLogger
}

func New(alephAlpaAPIKey string, logger logrus.FieldLogger) *alephalpha {
	return &alephalpha{
		alephAlphaAPIKey: alephAlpaAPIKey,
		httpClient:	  &http.Client{},
		logger: 	  logger,
	}
}

func (a *alephalpha) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)  {
	return a.Generate(ctx, cfg, prompt)
}

func (a *alephalpha) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	return a.Generate(ctx, cfg, task)
}

func (a *alephalpha) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	
	result := "a result from alephalpha /complete endpoint"

	return &generativemodels.GenerateResponse{
		Result: &result,
	}, nil
}
