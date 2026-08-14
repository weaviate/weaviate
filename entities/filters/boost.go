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

// Boost represents a set of soft ranking conditions that promote or demote
// matching documents without excluding non-matching ones. Boost rescores the
// primary search results using a weighted combination of primary and boost scores.
type Boost struct {
	Conditions []BoostCondition
	Weight     float32 // blending weight [0,1]: final = (1-w)*primary + w*boost, default 0.5
	Depth      int     // candidate pool size for reranking; 0 means use default (100)

	// OriginalOffset and OriginalLimit are set internally by the explorer
	// when overfetching for boost. They record the user's requested pagination
	// so it can be applied after boost re-sorts.
	OriginalOffset int
	OriginalLimit  int
}

// PropertyValueModifierType is the modifier applied to a property value before normalization.
type PropertyValueModifierType string

const (
	PropertyValueModifierNone  PropertyValueModifierType = "none"
	PropertyValueModifierLog1p PropertyValueModifierType = "log1p"
	PropertyValueModifierSqrt  PropertyValueModifierType = "sqrt"
)

// DecayCurveType is the mathematical function used for distance-based decay scoring.
type DecayCurveType string

const (
	DecayCurveExp    DecayCurveType = "exp"
	DecayCurveGauss  DecayCurveType = "gauss"
	DecayCurveLinear DecayCurveType = "linear"
)

// BoostCondition represents a single boost condition. Exactly one of
// Filter, Decay, or PropertyValue must be set.
type BoostCondition struct {
	Filter        *LocalFilter   // binary: 1 if match, 0 if not
	Decay         *Decay         // continuous: distance-based [0,1]
	PropertyValue *PropertyValue // continuous: score proportional to property value
	Weight        float32        // per-condition weight, default 1.0; negative values demote
}

// PropertyValue defines a scoring function that produces a score proportional to
// a numeric property's value. The raw values are normalized to [0,1] across
// the result set using min-max normalization after applying the modifier.
type PropertyValue struct {
	Path     *Path
	Modifier PropertyValueModifierType
}

// Decay defines a distance-based scoring function that produces a continuous
// score in [0,1] based on how far a property value is from an origin point.
//
// For time-based decay, Origin/Scale/Offset are strings ("now", "7d", ISO date).
// For numeric decay, set IsNumeric=true and use OriginNumeric/ScaleNumeric/OffsetNumeric
// directly to avoid lossy float64→string→float64 round-trips.
type Decay struct {
	Path   *Path
	Origin string // "now", ISO date, or numeric string (time decay)
	Scale  string // "7d", "20" — required (time decay)
	Offset string // default "0" (time decay)

	// Pre-parsed numeric values. When IsNumeric is true, these are used
	// directly by the scorer instead of parsing Origin/Scale/Offset strings.
	IsNumeric     bool
	OriginNumeric float64
	ScaleNumeric  float64
	OffsetNumeric float64

	Curve      DecayCurveType
	DecayValue float32 // score at scale distance, default 0.5
}

// MaxBoostConditions is the maximum number of conditions allowed in a single
// boost clause. Each condition triggers index queries or per-document scoring,
// so an unbounded count could cause resource exhaustion.
const MaxBoostConditions = 20

// ValidateBoost validates a Boost struct for correctness. Returns nil for
// nil input.
func ValidateBoost(boost *Boost) error {
	if boost == nil {
		return nil
	}

	if len(boost.Conditions) == 0 {
		return fmt.Errorf("boost: at least one condition is required")
	}

	if len(boost.Conditions) > MaxBoostConditions {
		return fmt.Errorf("boost: too many conditions (%d), maximum is %d",
			len(boost.Conditions), MaxBoostConditions)
	}

	if boost.Weight < 0 || boost.Weight > 1 {
		return fmt.Errorf("boost: weight must be between 0 and 1, got %f", boost.Weight)
	}

	if boost.Depth < 0 {
		return fmt.Errorf("boost: depth must be >= 0, got %d", boost.Depth)
	}

	for i, cond := range boost.Conditions {
		if err := validateBoostCondition(cond, i); err != nil {
			return err
		}
	}

	return nil
}

func validateBoostCondition(cond BoostCondition, idx int) error {
	hasFilter := cond.Filter != nil
	hasDecay := cond.Decay != nil
	hasPropertyValue := cond.PropertyValue != nil

	set := 0
	if hasFilter {
		set++
	}
	if hasDecay {
		set++
	}
	if hasPropertyValue {
		set++
	}
	if set != 1 {
		return fmt.Errorf("boost condition[%d]: exactly one of 'filter', 'decay', or 'property_value' must be set", idx)
	}

	if hasFilter {
		return validateBoostFilterOps(cond.Filter.Root, idx)
	}

	if hasDecay {
		return validateDecay(cond.Decay, idx)
	}

	if hasPropertyValue {
		return validatePropertyValue(cond.PropertyValue, idx)
	}

	return nil
}

// validateBoostFilterOps checks that the filter only uses operators supported
// by in-memory evaluation. Unsupported operators would silently score 0.
func validateBoostFilterOps(clause *Clause, condIdx int) error {
	if clause == nil {
		return nil
	}
	switch clause.Operator {
	case OperatorEqual, OperatorNotEqual,
		OperatorGreaterThan, OperatorGreaterThanEqual,
		OperatorLessThan, OperatorLessThanEqual,
		OperatorAnd, OperatorOr, OperatorNot:
		// supported
	default:
		return fmt.Errorf("boost condition[%d] filter: operator %s is not supported in boost conditions", condIdx, clause.Operator.Name())
	}
	for i := range clause.Operands {
		if err := validateBoostFilterOps(&clause.Operands[i], condIdx); err != nil {
			return err
		}
	}
	return nil
}

func validateDecay(d *Decay, condIdx int) error {
	if d.Path == nil {
		return fmt.Errorf("boost condition[%d] decay: path is required", condIdx)
	}
	// Origin is optional — defaults to "now" for date properties at scoring time.
	// For numeric properties, a missing origin will produce a parse error at scoring time.
	if d.IsNumeric {
		if d.ScaleNumeric <= 0 {
			return fmt.Errorf("boost condition[%d] decay: scale must be > 0", condIdx)
		}
	} else if d.Scale == "" {
		return fmt.Errorf("boost condition[%d] decay: scale is required", condIdx)
	}

	switch d.Curve {
	case DecayCurveExp, DecayCurveGauss, DecayCurveLinear, "":
		// valid
	default:
		return fmt.Errorf("boost condition[%d] decay: curve must be one of 'exp', 'gauss', 'linear', got %q", condIdx, d.Curve)
	}

	// DecayValue == 0 is treated as unset (defaults to 0.5 at scoring time).
	// Explicit values must be in (0, 1].
	if d.DecayValue != 0 && (d.DecayValue < 0 || d.DecayValue > 1) {
		return fmt.Errorf("boost condition[%d] decay: decay_value must be between 0 and 1, got %f", condIdx, d.DecayValue)
	}

	return nil
}

func validatePropertyValue(fv *PropertyValue, condIdx int) error {
	if fv.Path == nil {
		return fmt.Errorf("boost condition[%d] property_value: path is required", condIdx)
	}
	switch fv.Modifier {
	case PropertyValueModifierNone, PropertyValueModifierLog1p, PropertyValueModifierSqrt, "":
		// valid
	default:
		return fmt.Errorf("boost condition[%d] property_value: modifier must be one of 'none', 'log1p', 'sqrt', got %q", condIdx, fv.Modifier)
	}
	return nil
}
