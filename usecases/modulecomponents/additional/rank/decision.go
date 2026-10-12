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

package rank

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	rerankmodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

// Order is the order of the results a decision model reranks.
type Order string

const (
	// OrderSearch keeps the order of the search; the probability only decides
	// which results are dropped.
	OrderSearch Order = "search"
	// OrderProbability sorts by the probability, highest first.
	OrderProbability Order = "probability"
)

func ValidOrder(order Order) bool {
	return order == OrderSearch || order == OrderProbability
}

// Decision are the options of a rerank by a decision model: the module
// answers the query as a predicate about every candidate, and the probability
// is the score. A nil field takes the class setting of the module.
type Decision struct {
	// MinProbability drops the candidates whose probability is below it.
	MinProbability *float64
	Order          *Order
}

// Validate rejects a probability outside [0, 1] and an unknown order.
func (d *Decision) Validate() error {
	if d == nil {
		return nil
	}
	if d.MinProbability != nil && !(*d.MinProbability >= 0 && *d.MinProbability <= 1) {
		return fmt.Errorf("rerank.decision.min_probability must be between 0 and 1, got %v", *d.MinProbability)
	}
	if d.Order != nil && !ValidOrder(*d.Order) {
		return fmt.Errorf("rerank.decision.order must be %q or %q, got %q", OrderSearch, OrderProbability, *d.Order)
	}
	return nil
}

// DecisionSettings are the class settings of a decisions module that a
// rerank uses where the request leaves a Decision field unset.
type DecisionSettings struct {
	MinProbability float64
	Order          Order
}

// DecisionDefaults reads the DecisionSettings of a class. A decisions module
// supplies it; a reranker module has none, its results are sorted by score
// and none is dropped.
type DecisionDefaults func(cfg moduletools.ClassConfig) (DecisionSettings, error)

// resolveDecision applies the request's Decision over the class settings.
func resolveDecision(decision *Decision, settings DecisionSettings) DecisionSettings {
	if decision == nil {
		return settings
	}
	if decision.MinProbability != nil {
		settings.MinProbability = *decision.MinProbability
	}
	if decision.Order != nil {
		settings.Order = *decision.Order
	}
	return settings
}

// applyDecision orders the scored results and drops those below the
// threshold.
func applyDecision(scored []search.Result, settings DecisionSettings) ([]search.Result, error) {
	if settings.Order == OrderProbability {
		SortByScore(scored)
	}
	if settings.MinProbability <= 0 {
		return scored, nil
	}
	kept := make([]search.Result, 0, len(scored))
	for _, result := range scored {
		score, err := rerankScore(result)
		if err != nil {
			return nil, err
		}
		if score >= settings.MinProbability {
			kept = append(kept, result)
		}
	}
	return kept, nil
}

func rerankScore(result search.Result) (float64, error) {
	scores, ok := result.AdditionalProperties["rerank"].([]*rerankmodels.RankResult)
	if !ok || len(scores) == 0 || scores[0] == nil || scores[0].Score == nil {
		return 0, fmt.Errorf("result %s has no rerank score", result.ID)
	}
	return *scores[0].Score, nil
}
