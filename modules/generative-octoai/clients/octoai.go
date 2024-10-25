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
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

type octoai struct {
	apiKey     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *octoai {
	return &octoai{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *octoai) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	return nil, errors.New("OctoAI is permanently shut down")
}

func (v *octoai) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error) {
	return nil, errors.New("OctoAI is permanently shut down")
}

func (v *octoai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error) {
	return nil, errors.New("OctoAI is permanently shut down")
}
