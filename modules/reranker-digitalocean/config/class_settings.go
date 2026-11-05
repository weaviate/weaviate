//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultBaseURL = "https://inference.do-ai.run"
	DefaultModel   = "bge-reranker-v2-m3"
)

// ModelLister lists the model ids available on a DigitalOcean Serverless Inference endpoint.
type ModelLister interface {
	ListModels(ctx context.Context, baseURL, apiKey string, weaviateUUID string) ([]string, error)
}

// DefaultModelLister is registered by the clients package at init time.
var DefaultModelLister ModelLister

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("reranker-digitalocean")}
}

func (cs *classSettings) BaseURL() string {
	return cs.propertyValuesHelper.GetPropertyAsString(cs.cfg, "baseURL", DefaultBaseURL)
}

func (cs *classSettings) Model() string {
	return cs.propertyValuesHelper.GetPropertyAsString(cs.cfg, "model", DefaultModel)
}

func (cs *classSettings) apiKey() string {
	return os.Getenv("DIGITALOCEAN_APIKEY")
}

func (cs *classSettings) WeaviateUUID() string {
	return os.Getenv("VECTOR_DB_UUID")
}

// Validate checks the model against /v1/models only when DIGITALOCEAN_APIKEY is set.
func (cs *classSettings) Validate(ctx context.Context, class *models.Class) error {
	if err := cs.propertyValuesHelper.ValidateBaseURL(cs.BaseURL()); err != nil {
		return err
	}

	lister := DefaultModelLister
	apiKey := cs.apiKey()
	if lister == nil || apiKey == "" {
		return nil
	}

	model := cs.Model()
	available, err := lister.ListModels(ctx, cs.BaseURL(), apiKey, cs.WeaviateUUID())
	if err != nil {
		return errors.Wrap(err, "list DigitalOcean models")
	}
	if !slices.Contains(available, model) {
		return fmt.Errorf("model %q is not available on the DigitalOcean Serverless Inference endpoint; available models: %v", model, available)
	}
	return nil
}
