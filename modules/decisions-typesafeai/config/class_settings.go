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
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const ModuleName = "decisions-typesafeai"

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
	// MaxDocumentsLimit is the ceiling of maxDocuments, the class setting and
	// the request option that overrides it.
	MaxDocumentsLimit = 1000
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper(ModuleName)}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	// The settings helper reads a value of the wrong type as the default;
	// a request option or a class setting of the wrong type is a mistake.
	for _, name := range []string{"model", "baseURL", "order"} {
		if value, ok := ic.raw(name); ok {
			if _, isString := value.(string); !isString {
				return fmt.Errorf("%s must be a string, got %v", name, value)
			}
		}
	}
	if model := ic.Model(); model == "" {
		return errors.New("no model provided")
	}
	if maxDocuments := ic.MaxDocuments(); maxDocuments < 1 || maxDocuments > MaxDocumentsLimit {
		stored, _ := ic.raw("maxDocuments")
		return fmt.Errorf("maxDocuments must be a whole number between 1 and %d, got %v", MaxDocumentsLimit, stored)
	}
	if order := ic.Order(); !ValidOrder(order) {
		return fmt.Errorf("order must be %q or %q, got %q", OrderSearch, OrderProbability, order)
	}
	if minProbability := ic.MinProbability(); !ValidProbability(minProbability) {
		return fmt.Errorf("minProbability must be between 0 and 1, got %v", minProbability)
	}
	if _, err := ic.Images(); err != nil {
		return err
	}
	return ic.propertyValuesHelper.ValidateBaseURL(ic.BaseURL())
}

// Images is the class's opt-in to the API's experimental image input.
func (ic *classSettings) Images() (bool, error) {
	if ic.cfg == nil {
		return false, nil
	}
	value, ok := ic.cfg.ClassByModuleName(ModuleName)["images"]
	if !ok {
		return false, nil
	}
	images, isBool := value.(bool)
	if !isBool {
		return false, fmt.Errorf("images must be true or false, got %v", value)
	}
	return images, nil
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
// MaxDocuments is the setting, or the default when unset. A value that is
// not a whole number maps to 0, which Validate and DecisionLimits reject.
func (ic *classSettings) MaxDocuments() int {
	value, ok := ic.raw("maxDocuments")
	if !ok {
		return DefaultMaxDocuments
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		if v == math.Trunc(v) && math.Abs(v) <= math.MaxInt32 {
			return int(v)
		}
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return int(n)
		}
	}
	return 0
}

// raw is the setting as the class or the request holds it.
func (ic *classSettings) raw(name string) (any, bool) {
	if ic.cfg == nil {
		return nil, false
	}
	value, ok := ic.cfg.ClassByModuleName(ModuleName)[name]
	return value, ok
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
