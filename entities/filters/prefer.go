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

package filters

import (
	"fmt"
)

// Prefer represents a set of soft preference conditions that promote matching
// documents without excluding non-matching ones. Prefer rescores the primary
// search results using a weighted combination of primary and preference scores.
type Prefer struct {
	Conditions []PreferCondition
	Strength   float32 // blending weight [0,1]: final = (1-s)*primary + s*prefer, default 0.5
}

// PreferCondition represents a single preference condition. Exactly one of
// Filter or Decay must be set.
type PreferCondition struct {
	Filter *LocalFilter // binary: 1 if match, 0 if not
	Decay  *Decay       // continuous: distance-based [0,1]
	Weight float32      // per-condition weight, default 1.0
}

// Decay defines a distance-based scoring function that produces a continuous
// score in [0,1] based on how far a property value is from an origin point.
type Decay struct {
	Path       *Path
	Origin     string  // "now", ISO date, or numeric string
	Scale      string  // "7d", "20" — required
	Offset     string  // default "0"
	Curve      string  // "exp" (default), "gauss", "linear"
	DecayValue float32 // score at scale distance, default 0.5
}

// MaxPreferConditions is the maximum number of conditions allowed in a single
// prefer clause. Each condition triggers index queries or per-document scoring,
// so an unbounded count could cause resource exhaustion.
const MaxPreferConditions = 20

// ValidatePrefer validates a Prefer struct for correctness. Returns nil for
// nil input.
func ValidatePrefer(prefer *Prefer) error {
	if prefer == nil {
		return nil
	}

	if len(prefer.Conditions) == 0 {
		return fmt.Errorf("prefer: at least one condition is required")
	}

	if len(prefer.Conditions) > MaxPreferConditions {
		return fmt.Errorf("prefer: too many conditions (%d), maximum is %d",
			len(prefer.Conditions), MaxPreferConditions)
	}

	if prefer.Strength < 0 || prefer.Strength > 1 {
		return fmt.Errorf("prefer: strength must be between 0 and 1, got %f", prefer.Strength)
	}

	for i, cond := range prefer.Conditions {
		if err := validatePreferCondition(cond, i); err != nil {
			return err
		}
	}

	return nil
}

func validatePreferCondition(cond PreferCondition, idx int) error {
	hasFilter := cond.Filter != nil
	hasDecay := cond.Decay != nil

	if !hasFilter && !hasDecay {
		return fmt.Errorf("prefer condition[%d]: exactly one of 'filter' or 'decay' must be set", idx)
	}
	if hasFilter && hasDecay {
		return fmt.Errorf("prefer condition[%d]: exactly one of 'filter' or 'decay' must be set, both are set", idx)
	}

	if cond.Weight < 0 {
		return fmt.Errorf("prefer condition[%d]: weight must be >= 0, got %f", idx, cond.Weight)
	}

	if hasFilter {
		if err := validatePreferFilterOps(cond.Filter.Root, idx); err != nil {
			return err
		}
	}

	if hasDecay {
		return validateDecay(cond.Decay, idx)
	}

	return nil
}

// validatePreferFilterOps checks that the filter only uses operators supported
// by in-memory evaluation. Unsupported operators would silently score 0.
func validatePreferFilterOps(clause *Clause, condIdx int) error {
	if clause == nil {
		return nil
	}
	switch clause.Operator {
	case OperatorWithinGeoRange:
		return fmt.Errorf("prefer condition[%d] filter: operator WithinGeoRange is not supported in prefer conditions", condIdx)
	case ContainsAny:
		return fmt.Errorf("prefer condition[%d] filter: operator ContainsAny is not supported in prefer conditions", condIdx)
	case ContainsAll:
		return fmt.Errorf("prefer condition[%d] filter: operator ContainsAll is not supported in prefer conditions", condIdx)
	case ContainsNone:
		return fmt.Errorf("prefer condition[%d] filter: operator ContainsNone is not supported in prefer conditions", condIdx)
	}
	for i := range clause.Operands {
		if err := validatePreferFilterOps(&clause.Operands[i], condIdx); err != nil {
			return err
		}
	}
	return nil
}

func validateDecay(d *Decay, condIdx int) error {
	if d.Path == nil {
		return fmt.Errorf("prefer condition[%d] decay: path is required", condIdx)
	}
	if d.Origin == "" {
		return fmt.Errorf("prefer condition[%d] decay: origin is required", condIdx)
	}
	if d.Scale == "" {
		return fmt.Errorf("prefer condition[%d] decay: scale is required", condIdx)
	}

	switch d.Curve {
	case "exp", "gauss", "linear", "":
		// valid
	default:
		return fmt.Errorf("prefer condition[%d] decay: curve must be one of 'exp', 'gauss', 'linear', got %q", condIdx, d.Curve)
	}

	// DecayValue == 0 is treated as unset (defaults to 0.5 at scoring time).
	// Explicit values must be in (0, 1].
	if d.DecayValue != 0 && (d.DecayValue < 0 || d.DecayValue > 1) {
		return fmt.Errorf("prefer condition[%d] decay: decay_value must be between 0 and 1, got %f", condIdx, d.DecayValue)
	}

	return nil
}
