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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

type octoai struct{}

func New(apiKey string, timeout time.Duration, logger logrus.FieldLogger) *octoai {
	return &octoai{}
}

func (v *octoai) GenerateSingleResult(ctx context.Context, properties *modulecapabilities.GenerateProperties, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return nil, errors.New("OctoAI is permanently shut down")
}

func (v *octoai) GenerateAllResults(ctx context.Context, properties []*modulecapabilities.GenerateProperties, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	return nil, errors.New("OctoAI is permanently shut down")
}

func (v *octoai) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	return nil, errors.New("OctoAI is permanently shut down")
}
