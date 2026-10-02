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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	baseURLProperty          = "baseURL"
	modelProperty            = "model"
	temperatureProperty      = "temperature"
	topPProperty             = "topP"
	maxTokensProperty        = "maxTokens"
	frequencyPenaltyProperty = "frequencyPenalty"
	presencePenaltyProperty  = "presencePenalty"
	stopProperty             = "stop"
)

var (
	DefaultBaseURL = "https://inference.do-ai.run"
	DefaultModel   = "llama-4-maverick"
)

// ModelLister returns the set of available model ids from a DigitalOcean
// Serverless Inference endpoint. It is implemented in the clients package and
// injected here through DefaultModelLister to keep config free of HTTP-client
// dependencies. Tests can override DefaultModelLister with a fake.
type ModelLister interface {
	ListModels(ctx context.Context, baseURL, apiKey string, weaviateUUID string) ([]string, error)
}

// DefaultModelLister is the lister used by Validate. The clients package
// registers an HTTP-backed implementation at init time.
var DefaultModelLister ModelLister

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-digitalocean")}
}

func (ic *classSettings) Validate(ctx context.Context, class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}
	if err := ic.propertyValuesHelper.ValidateBaseURL(ic.BaseURL()); err != nil {
		return err
	}
	if temperature := ic.Temperature(); temperature != nil && (*temperature < 0 || *temperature > 2) {
		return errors.New("wrong temperature configuration, values are between 0.0 and 2.0")
	}
	if topP := ic.TopP(); topP != nil && (*topP < 0 || *topP > 1) {
		return errors.New("wrong topP configuration, values are between 0.0 and 1.0")
	}
	if maxTokens := ic.MaxTokens(); maxTokens != nil && *maxTokens < 1 {
		return errors.New("wrong maxTokens configuration, values have a minimal value of 1")
	}
	if frequencyPenalty := ic.FrequencyPenalty(); frequencyPenalty != nil && (*frequencyPenalty < -2 || *frequencyPenalty > 2) {
		return errors.New("wrong frequencyPenalty configuration, values are between -2.0 and 2.0")
	}
	if presencePenalty := ic.PresencePenalty(); presencePenalty != nil && (*presencePenalty < -2 || *presencePenalty > 2) {
		return errors.New("wrong presencePenalty configuration, values are between -2.0 and 2.0")
	}
	return ic.validateModel(ctx)
}

func (ic *classSettings) validateModel(ctx context.Context) error {
	lister := DefaultModelLister
	if lister == nil {
		return nil
	}

	apiKey := ic.apiKey()
	if apiKey == "" {
		// Without a server-side API key the model can't be checked against
		// /v1/models; the endpoint rejects an unknown model at generate time,
		// where users can supply their own key via X-Digitalocean-Api-Key.
		return nil
	}

	model := ic.Model()
	available, err := lister.ListModels(ctx, ic.BaseURL(), apiKey, ic.WeaviateUUID())
	if err != nil {
		return errors.Wrap(err, "list DigitalOcean models")
	}

	for _, id := range available {
		if id == model {
			return nil
		}
	}

	return fmt.Errorf("model %q is not available on the DigitalOcean Serverless Inference endpoint; available models: %v", model, available)
}

// apiKey resolves the DigitalOcean API key from the DIGITALOCEAN_APIKEY
// environment variable. Validation runs at collection-create time, where the
// per-request header is not available, so only the server-level env var is
// consulted.
func (ic *classSettings) apiKey() string {
	return os.Getenv("DIGITALOCEAN_APIKEY")
}

func (ic *classSettings) WeaviateUUID() string {
	return os.Getenv("VECTOR_DB_UUID")
}

func (ic *classSettings) BaseURL() string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, baseURLProperty, DefaultBaseURL)
}

func (ic *classSettings) Model() string {
	return ic.propertyValuesHelper.GetPropertyAsString(ic.cfg, modelProperty, DefaultModel)
}

func (ic *classSettings) Temperature() *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, temperatureProperty, nil)
}

func (ic *classSettings) TopP() *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, topPProperty, nil)
}

func (ic *classSettings) MaxTokens() *int {
	return ic.propertyValuesHelper.GetPropertyAsInt(ic.cfg, maxTokensProperty, nil)
}

func (ic *classSettings) FrequencyPenalty() *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, frequencyPenaltyProperty, nil)
}

func (ic *classSettings) PresencePenalty() *float64 {
	return ic.propertyValuesHelper.GetPropertyAsFloat64(ic.cfg, presencePenaltyProperty, nil)
}

func (ic *classSettings) Stop() []string {
	return ic.propertyValuesHelper.GetPropertyAsListOfStrings(ic.cfg, stopProperty, nil)
}
