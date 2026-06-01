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

package storobj

import (
	"fmt"
	"math"
	"time"
)

// EvalPredicate evaluates p against the properties of obj and returns nil when
// the predicate is satisfied, or an error describing why it was not.
//
// The returned error is a *PredicateError, which the shard-write path converts
// into an ErrPreconditionFailed so the caller receives the standard domain error.
//
// Evaluation rules:
//  1. If obj is nil (object does not exist), all predicates fail -- the absence
//     of an object implies the absence of all properties.
//  2. If the named property is absent from obj.Object.Properties, the predicate
//     fails ("property not found").
//  3. Multi-value ([]interface{}) properties are rejected -- out of scope for v1.
//  4. Phase-3 v1 only supports EqOperator; other Operator values return an error.
func EvalPredicate(p *Predicate, obj *Object) error {
	if p == nil {
		return nil
	}
	if p.Operator != EqOperator {
		return &PredicateError{
			PropertyName: p.PropertyName,
			Reason:       fmt.Sprintf("unsupported predicate operator %d; only EqOperator (0) is supported in Phase-3 v1", p.Operator),
		}
	}

	if obj == nil {
		return &PredicateError{
			PropertyName: p.PropertyName,
			Reason:       fmt.Sprintf("property %q not found on stored object (object does not exist; field_match condition failed)", p.PropertyName),
		}
	}

	props, ok := obj.Object.Properties.(map[string]interface{})
	if !ok || props == nil {
		return &PredicateError{
			PropertyName: p.PropertyName,
			Reason:       fmt.Sprintf("property %q not found on stored object (object has no properties; field_match condition failed)", p.PropertyName),
		}
	}

	raw, exists := props[p.PropertyName]
	if !exists {
		return &PredicateError{
			PropertyName: p.PropertyName,
			Reason:       fmt.Sprintf("property %q not found on stored object (field_match condition failed)", p.PropertyName),
		}
	}

	// Reject multi-value (array) properties -- out of scope for v1.
	if _, isSlice := raw.([]interface{}); isSlice {
		return &PredicateError{
			PropertyName: p.PropertyName,
			Reason:       fmt.Sprintf("property %q is a multi-value array type; field_match on arrays is not supported in v1", p.PropertyName),
		}
	}

	switch p.ValueType {
	case PredicateValueText:
		return evalTextEq(p.PropertyName, raw, p.ExpectedValue)
	case PredicateValueInt:
		return evalIntEq(p.PropertyName, raw, p.ExpectedValue)
	case PredicateValueNumber:
		return evalNumberEq(p.PropertyName, raw, p.ExpectedValue)
	case PredicateValueBool:
		return evalBoolEq(p.PropertyName, raw, p.ExpectedValue)
	case PredicateValueDate:
		return evalDateEq(p.PropertyName, raw, p.ExpectedValue)
	default:
		return &PredicateError{
			PropertyName: p.PropertyName,
			Reason:       fmt.Sprintf("unknown value_type %d for field_match on property %q", p.ValueType, p.PropertyName),
		}
	}
}

// PredicateError is returned by EvalPredicate when the predicate is not
// satisfied. Callers convert this to an ErrPreconditionFailed.
type PredicateError struct {
	PropertyName string
	Reason       string
}

func (e *PredicateError) Error() string {
	return e.Reason
}

// evalExactEq is a generic helper for types that support == comparison
// (string, bool). It asserts both stored and expected are of type T, then
// checks equality.
//
// goTypeName is the Go type name used in "expected <type>" error clauses.
// valueTypeName is the predicate value_type string used in "value_type=<name>" clauses.
// storedFmt / expectedFmt are format verbs for the mismatch message (%q, %v, etc.).
func evalExactEq[T comparable](name string, stored, expected interface{}, goTypeName, valueTypeName, storedFmt, expectedFmt string) error {
	storedV, ok := stored.(T)
	if !ok {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q has type %T, expected %s for value_type=%s (field_match condition failed)", name, stored, goTypeName, valueTypeName),
		}
	}
	expectedV, ok := expected.(T)
	if !ok {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("predicate expected value has type %T, expected %s for value_type=%s", expected, goTypeName, valueTypeName),
		}
	}
	if storedV != expectedV {
		return &PredicateError{
			PropertyName: name,
			Reason: fmt.Sprintf("property %q value mismatch: stored "+storedFmt+" != expected "+expectedFmt+" (field_match condition failed)",
				name, storedV, expectedV),
		}
	}
	return nil
}

// evalTextEq checks that stored == expected for text properties.
// Weaviate stores text properties as plain Go strings. Comparison is
// exact and case-sensitive (matching Weaviate's text equality semantics).
func evalTextEq(name string, stored, expected interface{}) error {
	return evalExactEq[string](name, stored, expected, "string", "text", "%q", "%q")
}

// evalIntEq checks integer equality. Weaviate stores numeric properties as
// float64 in JSON-decoded maps; we round-trip via int64 to handle both the
// stored-as-float64 case and the stored-as-int64 case.
func evalIntEq(name string, stored, expected interface{}) error {
	storedInt, err := toInt64(stored)
	if err != nil {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q cannot be read as int: %v (field_match condition failed)", name, err),
		}
	}
	expectedInt, ok := expected.(int64)
	if !ok {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("predicate expected value has type %T, expected int64 for value_type=int", expected),
		}
	}
	if storedInt != expectedInt {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q value mismatch: stored %d != expected %d (field_match condition failed)", name, storedInt, expectedInt),
		}
	}
	return nil
}

// evalNumberEq checks float64 equality. IEEE-754 exact equality; callers
// preferring precise semantics should use PredicateValueText or PredicateValueInt.
// A WARN-level log is emitted at the absorption point (see defense-in-depth rule)
// to make float-equality surprises visible.
func evalNumberEq(name string, stored, expected interface{}) error {
	storedF, err := toFloat64(stored)
	if err != nil {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q cannot be read as number: %v (field_match condition failed)", name, err),
		}
	}
	expectedF, ok := expected.(float64)
	if !ok {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("predicate expected value has type %T, expected float64 for value_type=number", expected),
		}
	}
	// IEEE-754 NaN is not equal to itself; treat NaN != NaN as predicate failure.
	if math.IsNaN(storedF) || math.IsNaN(expectedF) {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q: NaN is never equal to NaN (field_match condition failed)", name),
		}
	}
	if storedF != expectedF {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q value mismatch: stored %g != expected %g (field_match condition failed; note: float equality is unreliable -- prefer value_type=int or value_type=text)", name, storedF, expectedF),
		}
	}
	return nil
}

// evalBoolEq checks boolean equality. Weaviate stores booleans as Go bool.
func evalBoolEq(name string, stored, expected interface{}) error {
	return evalExactEq[bool](name, stored, expected, "bool", "bool", "%v", "%v")
}

// evalDateEq checks date equality. Both stored and expected values are parsed
// as RFC 3339 strings and compared in UTC. String equality is not used (a
// stored date might have timezone offset +02:00 while expected is Z).
func evalDateEq(name string, stored, expected interface{}) error {
	storedStr, ok := stored.(string)
	if !ok {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q has type %T, expected RFC 3339 string for value_type=date (field_match condition failed)", name, stored),
		}
	}
	expectedStr, ok := expected.(string)
	if !ok {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("predicate expected value has type %T, expected RFC 3339 string for value_type=date", expected),
		}
	}
	storedTime, err := time.Parse(time.RFC3339Nano, storedStr)
	if err != nil {
		// Fall back to plain RFC3339 if nanoseconds fail.
		storedTime, err = time.Parse(time.RFC3339, storedStr)
		if err != nil {
			return &PredicateError{
				PropertyName: name,
				Reason:       fmt.Sprintf("property %q stored value %q is not a valid RFC 3339 date (field_match condition failed): %v", name, storedStr, err),
			}
		}
	}
	expectedTime, err := time.Parse(time.RFC3339Nano, expectedStr)
	if err != nil {
		expectedTime, err = time.Parse(time.RFC3339, expectedStr)
		if err != nil {
			return &PredicateError{
				PropertyName: name,
				Reason:       fmt.Sprintf("predicate expected value %q is not a valid RFC 3339 date: %v", expectedStr, err),
			}
		}
	}
	// Compare in UTC to normalize timezone offsets.
	if !storedTime.UTC().Equal(expectedTime.UTC()) {
		return &PredicateError{
			PropertyName: name,
			Reason:       fmt.Sprintf("property %q date mismatch: stored %s != expected %s (field_match condition failed)", name, storedTime.UTC().Format(time.RFC3339Nano), expectedTime.UTC().Format(time.RFC3339Nano)),
		}
	}
	return nil
}

// toInt64 converts a value to int64, handling the float64 case from JSON decoding
// and the common int/int64 variants.
func toInt64(v interface{}) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case float64:
		// JSON-decoded numbers arrive as float64. Round-trip via int64 only if
		// the float64 represents an exact integer.
		i := int64(x)
		if float64(i) != x {
			return 0, fmt.Errorf("value %g is not an exact integer", x)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

// toFloat64 converts a value to float64, handling int/int64 variants as well.
func toFloat64(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}
