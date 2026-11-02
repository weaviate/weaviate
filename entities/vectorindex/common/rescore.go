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

package common

import (
	"encoding/json"
	"fmt"
	"math"
)

// RescoreFactorAuto is the stored default of rescoreFactor: the index type
// picks the quantizer's own factor. 0 resolves the same way.
const RescoreFactorAuto = -1.0

// RescorePolicy is the resolved rescore setting of one quantizer: how many of
// the best candidates by compressed distance a search re-ranks with their
// uncompressed vectors.
type RescorePolicy struct {
	// Limit is the minimum number of candidates rescored. hnsw treats 0 as
	// rescoring disabled; flat always rescores at least k.
	Limit int
	// Factor is the number of candidates rescored per requested result.
	Factor float64
}

// Candidates returns how many of the best compressed candidates to rescore for
// k results: max(k, Limit, ceil(k * Factor)). hnsw bounds it by the candidates
// its graph search found, at most ef.
func (p RescorePolicy) Candidates(k int) int {
	// a factor that overflows an int would rescore every candidate anyway
	scaled := min(math.Ceil(float64(k)*p.Factor), math.MaxInt32)
	return max(k, p.Limit, int(scaled))
}

// ResolveRescoreFactor returns configured, or auto when configured is
// automatic (-1) or the zero value.
func ResolveRescoreFactor(configured, auto float64) float64 {
	if configured < 1 {
		return auto
	}
	return configured
}

// ParseRescoreFactor reads an optional rescoreFactor key. -1 is automatic,
// and so is 0, the zero value a config built in code carries: stored schemas
// pass through this parser on every restore, so rejecting it would keep the
// class from loading. Anything else must be a finite number of at least 1: a
// search always rescores at least k candidates, so a smaller factor would mean
// nothing.
func ParseRescoreFactor(in map[string]interface{}, quantizer string, setFn func(v float64)) error {
	value, ok := in["rescoreFactor"]
	if !ok {
		return nil
	}

	var factor float64
	switch typed := value.(type) {
	case json.Number:
		f, err := typed.Float64()
		if err != nil {
			return fmt.Errorf("%s rescoreFactor: %w", quantizer, err)
		}
		factor = f
	case float64:
		factor = typed
	case int:
		factor = float64(typed)
	case int64:
		factor = float64(typed)
	default:
		return fmt.Errorf("%s rescoreFactor must be a number, got %T", quantizer, value)
	}

	if factor != RescoreFactorAuto && factor != 0 && (!(factor >= 1) || math.IsInf(factor, 1)) {
		return fmt.Errorf("%s rescoreFactor must be %v (automatic) or a number of at least 1, got %v",
			quantizer, RescoreFactorAuto, factor)
	}
	setFn(factor)
	return nil
}
