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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const moduleName = "decisions-typesafeai"

const (
	DefaultBaseURL         = "https://api.typesafe.ai"
	DefaultTypeSafeAIModel = "jev-latest"
	DefaultMaxDocuments    = 100
	DefaultMinProbability  = 0.0
	DefaultFetchDepth      = 0

	// OrderSearch keeps the results in the order the search gave them: the
	// probability only decides which ones are dropped. OrderProbability
	// sorts them by probability, highest first. With OrderProbability, or
	// with the cache off, the pages of one query are cut from candidate
	// sets judged separately, so a document can appear on two pages or on
	// none; pages are only consistent with the cache on and OrderSearch.
	OrderSearch      = "search"
	OrderProbability = "probability"
	DefaultOrder     = OrderSearch

	// A rubric has between MinScoreLevels and MaxScoreLevels levels, which is
	// what the TypeSafeAI API accepts.
	MinScoreLevels      = 2
	MaxScoreLevels      = 10
	MaxScoreLevelLength = 200
	DefaultMinScore     = 0.0
	ScoreLevelsHeader   = "X-Typesafeai-Score-Levels"
	// MaxDocumentsLimit bounds the per-query API spend.
	MaxDocumentsLimit = 1000
	// DefaultBatchSize is 1 because a document judged together with others
	// gets a less stable probability: measured on the same document, it
	// stayed within 0.04 alone and moved by up to 0.21 in batches of 10.
	DefaultBatchSize = 1
	// MaxBatchSize is where accuracy is reported to drop when more documents
	// share one request.
	MaxBatchSize = 25
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
	if batchSize := ic.BatchSize(); batchSize < 1 || batchSize > MaxBatchSize {
		return fmt.Errorf("batchSize must be between 1 and %d, got %d", MaxBatchSize, batchSize)
	}
	if fetchDepth := ic.FetchDepth(); fetchDepth < 0 || fetchDepth > ic.MaxDocuments() {
		return fmt.Errorf("fetchDepth must be between 0 and maxDocuments %d, got %d", ic.MaxDocuments(), fetchDepth)
	}
	levels, err := ic.ScoreLevels()
	if err != nil {
		return err
	}
	highestScore := MaxScoreLevels - 1
	if len(levels) > 0 {
		if err := validateScoreLevels("scoreLevels", levels); err != nil {
			return err
		}
		highestScore = len(levels) - 1
	}
	// Against the class's own rubric when it has one: a minScore that no
	// score can reach would fail every query.
	if minScore := ic.MinScore(); !(minScore >= 0 && minScore <= float64(highestScore)) {
		return fmt.Errorf("minScore must be between 0 and %d, got %v", highestScore, minScore)
	}
	if minProbability := ic.MinProbability(); !ValidProbability(minProbability) {
		return fmt.Errorf("minProbability must be between 0 and 1, got %v", minProbability)
	}
	return ic.propertyValuesHelper.ValidateBaseURL(ic.BaseURL())
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

// BatchSize is how many documents share one request to the TypeSafeAI API.
func (ic *classSettings) BatchSize() int {
	defaultValue := DefaultBatchSize
	// A value of the wrong type maps to 0, which Validate rejects.
	wrongValue := 0
	return *ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, "batchSize", &wrongValue, &defaultValue)
}

// FetchDepth is how many candidates a search fetches before the rerank, so
// the page can be filled after results are dropped. 0 fetches the page only.
func (ic *classSettings) FetchDepth() int {
	defaultValue := DefaultFetchDepth
	// A value of the wrong type maps to -1, which Validate rejects.
	wrongValue := -1
	return *ic.propertyValuesHelper.GetPropertyAsIntWithNotExists(ic.cfg, "fetchDepth", &wrongValue, &defaultValue)
}

// MinProbability is the threshold below which a result is dropped from the
// response. 0 keeps every result.
func (ic *classSettings) MinProbability() float64 {
	defaultValue := DefaultMinProbability
	// A value of the wrong type maps to -1, which Validate rejects.
	wrongValue := -1.0
	return *ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, "minProbability", &wrongValue, &defaultValue)
}

// ScoreLevels is the rubric of a score question, lowest level first. Without
// levels the module asks a yes/no question. A setting that is not a list of
// strings is an error: ignoring it would silently change the question type.
func (ic *classSettings) ScoreLevels() ([]string, error) {
	if ic.cfg == nil {
		return nil, nil
	}
	settings := ic.cfg.ClassByModuleName(moduleName)
	if len(settings) == 0 {
		settings = ic.cfg.Class()
	}
	switch value := settings["scoreLevels"].(type) {
	case nil:
		return nil, nil
	case []string:
		return slices.Clone(value), nil
	case []any:
		levels := make([]string, len(value))
		for i, level := range value {
			text, ok := level.(string)
			if !ok {
				return nil, fmt.Errorf("scoreLevels must be a list of level names, got %T at position %d", level, i+1)
			}
			levels[i] = text
		}
		return levels, nil
	default:
		return nil, fmt.Errorf("scoreLevels must be a list of level names, got %T", value)
	}
}

// MinScore is the score below which a result is dropped when the module asks
// a score question. A score is a position on the rubric, 0 for the first
// level. 0 keeps every result.
func (ic *classSettings) MinScore() float64 {
	defaultValue := DefaultMinScore
	// A value of the wrong type maps to -1, which Validate rejects.
	wrongValue := -1.0
	return *ic.propertyValuesHelper.GetPropertyAsFloat64WithNotExists(ic.cfg, "minScore", &wrongValue, &defaultValue)
}

// ScoreLevels returns the rubric from the request header, one level per "|",
// and falls back to the class setting. No levels means a yes/no question.
func ScoreLevels(ctx context.Context, cfg moduletools.ClassConfig) ([]string, error) {
	if header := modulecomponents.GetValueFromContext(ctx, ScoreLevelsHeader); header != "" {
		levels := strings.Split(header, "|")
		return levels, validateScoreLevels(ScoreLevelsHeader, levels)
	}
	levels, err := NewClassSettings(cfg).ScoreLevels()
	if err != nil || len(levels) == 0 {
		return nil, err
	}
	return levels, validateScoreLevels("scoreLevels", levels)
}

// validateScoreLevels trims the levels in place and rejects an empty, too
// long or repeated one: a repeated name would make the returned position
// ambiguous.
func validateScoreLevels(source string, levels []string) error {
	if len(levels) < MinScoreLevels || len(levels) > MaxScoreLevels {
		return fmt.Errorf("%s must list between %d and %d levels, got %d",
			source, MinScoreLevels, MaxScoreLevels, len(levels))
	}
	seen := make(map[string]int, len(levels))
	for i, level := range levels {
		level = strings.TrimSpace(level)
		levels[i] = level
		if level == "" {
			return fmt.Errorf("%s has an empty level at position %d", source, i+1)
		}
		if len(level) > MaxScoreLevelLength {
			return fmt.Errorf("%s has a level of %d bytes at position %d, the maximum is %d",
				source, len(level), i+1, MaxScoreLevelLength)
		}
		if first, ok := seen[level]; ok {
			return fmt.Errorf("%s repeats the level %q at positions %d and %d", source, level, first, i+1)
		}
		seen[level] = i + 1
	}
	return nil
}

func (ic *classSettings) getStringProperty(name string, defaultValue string) string {
	return ic.propertyValuesHelper.GetPropertyAsStringWithNotExists(ic.cfg, name, defaultValue, defaultValue)
}
