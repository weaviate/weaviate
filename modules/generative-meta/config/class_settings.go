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
	"slices"

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
	reasoningEffortProperty  = "reasoningEffort"
)

var (
	DefaultBaseURL = "https://api.meta.ai"
	DefaultModel   = "muse-spark-1.2"
)

var AvailableReasoningEfforts = []string{"none", "minimal", "low", "medium", "high", "xhigh"}

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper("generative-meta")}
}

func (ic *classSettings) Validate(class *models.Class) error {
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
	if err := ValidateReasoningEffort(ic.ReasoningEffort()); err != nil {
		return err
	}
	return nil
}

func ValidateReasoningEffort(reasoningEffort *string) error {
	if reasoningEffort == nil || slices.Contains(AvailableReasoningEfforts, *reasoningEffort) {
		return nil
	}
	return errors.Errorf("wrong reasoningEffort configuration, available values are: %v", AvailableReasoningEfforts)
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

func (ic *classSettings) ReasoningEffort() *string {
	if asString := ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, reasoningEffortProperty, "", ""); asString != "" {
		return &asString
	}
	return nil
}
