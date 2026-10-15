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
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const moduleName = "decisions-typesafeai"

const ModuleName = moduleName

const (
	DefaultBaseURL         = "https://api.typesafe.ai"
	DefaultTypeSafeAIModel = "jev-latest"
	DefaultMaxDocuments    = 100
	DefaultMinProbability  = 0.0

	// OrderSearch keeps the results in the order the search gave them: the
	// probability only decides which ones are dropped. OrderProbability
	// sorts them by probability, highest first. The pages of one query are
	// cut from candidate sets judged separately, so a document whose answer
	// is close to the threshold can appear on two pages or on none.
	OrderSearch      = "search"
	OrderProbability = "probability"
	DefaultOrder     = OrderSearch

	// MaxScoreLevels is how many levels a score question may have, which is
	// what the TypeSafeAI API accepts.
	MaxScoreLevels = 10
	// MaxDocumentsLimit bounds the per-query API spend.
	MaxDocumentsLimit = 1000
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper(moduleName)}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	if model := ic.Model(); model == "" {
		return errors.New("no model provided")
	}
	if maxDocuments := ic.MaxDocuments(); maxDocuments < 1 || maxDocuments > MaxDocumentsLimit {
		return fmt.Errorf("maxDocuments must be between 1 and %d, got %d", MaxDocumentsLimit, maxDocuments)
	}
	if order := ic.Order(); !ValidOrder(order) {
		return fmt.Errorf("order must be %q or %q, got %q", OrderSearch, OrderProbability, order)
	}
	if minProbability := ic.MinProbability(); !ValidProbability(minProbability) {
		return fmt.Errorf("minProbability must be between 0 and 1, got %v", minProbability)
	}
	if value, ok := ic.cfg.ClassByModuleName(moduleName)["images"]; ok {
		if _, isBool := value.(bool); !isBool {
			return fmt.Errorf("images must be true or false, got %v", value)
		}
	}
	return ic.propertyValuesHelper.ValidateBaseURL(ic.BaseURL())
}

// Images is the class's opt-in to the API's experimental image input.
func (ic *classSettings) Images() bool {
	if ic.cfg == nil {
		return false
	}
	value, _ := ic.cfg.ClassByModuleName(moduleName)["images"].(bool)
	return value
}

// ValidProbability reports whether p is in [0, 1]. NaN is not.
func ValidProbability(p float64) bool {
	return p >= 0 && p <= 1
}

// BaseURL is the API endpoint. An empty setting passes validation as "use
// the default", so it is the default here as well.
func (ic *classSettings) BaseURL() string {
	if url := ic.getStringProperty("baseURL", DefaultBaseURL); url != "" {
		return url
	}
	return DefaultBaseURL
}

func (ic *classSettings) Model() string {
	return ic.getStringProperty("model", DefaultTypeSafeAIModel)
}

// MaxDocuments is the highest number of documents a single rerank request may
// have judged by the TypeSafeAI API.
func (ic *classSettings) MaxDocuments() int {
	defaultValue := DefaultMaxDocuments
	// A value of the wrong type maps to 0, which Validate rejects.
	wrongValue := 0
	return *ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, "maxDocuments", &wrongValue, &defaultValue)
}

func ValidOrder(order string) bool {
	return order == OrderSearch || order == OrderProbability
}

// Order is the order of the results after the rerank: OrderSearch or
// OrderProbability.
func (ic *classSettings) Order() string {
	return ic.getStringProperty("order", DefaultOrder)
}

// MinProbability is the threshold below which a result is dropped from the
// response. 0 keeps every result.
func (ic *classSettings) MinProbability() float64 {
	defaultValue := DefaultMinProbability
	// A value of the wrong type maps to -1, which Validate rejects.
	wrongValue := -1.0
	return *ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, "minProbability", &wrongValue, &defaultValue)
}

func (ic *classSettings) getStringProperty(name string, defaultValue string) string {
	return ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, defaultValue, defaultValue)
}
