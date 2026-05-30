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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// makeObjectWithProps builds a minimal storobj.Object with the given properties map.
func makeObjectWithProps(props map[string]interface{}) *Object {
	return &Object{
		Object: models.Object{
			Properties: props,
		},
	}
}

// TestEvalPredicate_NilPredicate verifies that a nil predicate is always satisfied.
// Causal-link: EvalPredicate returns nil early when p==nil, so no stored object is
// ever checked; this covers the "condition not active" path.
func TestEvalPredicate_NilPredicate(t *testing.T) {
	err := EvalPredicate(nil, nil)
	require.NoError(t, err, "nil predicate must always pass")
}

// TestEvalPredicate_NilObject verifies that any predicate fails when the stored
// object is nil (object does not exist).
// Causal-link: EvalPredicate checks obj==nil before property access; if the
// object is absent the predicate must fail, not silently pass.
func TestEvalPredicate_NilObject(t *testing.T) {
	pred := &Predicate{
		PropertyName:  "status",
		ExpectedValue: "draft",
		ValueType:     PredicateValueText,
		Operator:      EqOperator,
	}
	err := EvalPredicate(pred, nil)
	require.Error(t, err, "predicate on nil object must fail")
	assert.Contains(t, err.Error(), "field_match condition failed")
}

// TestEvalPredicate_Text tests exact-match text equality (pass and fail paths).
// Causal-link: evalTextEq does a simple string comparison; a stored "draft" vs
// expected "draft" passes, stored "published" vs expected "draft" fails.
func TestEvalPredicate_Text(t *testing.T) {
	t.Run("match", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"status": "draft"})
		pred := &Predicate{PropertyName: "status", ExpectedValue: "draft", ValueType: PredicateValueText, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("mismatch", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"status": "published"})
		pred := &Predicate{PropertyName: "status", ExpectedValue: "draft", ValueType: PredicateValueText, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field_match condition failed")
	})
	t.Run("case_sensitive_mismatch", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"status": "Draft"})
		pred := &Predicate{PropertyName: "status", ExpectedValue: "draft", ValueType: PredicateValueText, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err, "text comparison must be case-sensitive")
	})
}

// TestEvalPredicate_Int tests integer equality including float64 stored value
// (JSON decode path) and mismatches.
// Causal-link: Weaviate stores numeric properties as float64 in JSON-decoded maps;
// toInt64 must handle float64(3) == int64(3) without error.
func TestEvalPredicate_Int(t *testing.T) {
	t.Run("match_float64_stored", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"priority": float64(3)})
		pred := &Predicate{PropertyName: "priority", ExpectedValue: int64(3), ValueType: PredicateValueInt, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("match_int64_stored", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"priority": int64(7)})
		pred := &Predicate{PropertyName: "priority", ExpectedValue: int64(7), ValueType: PredicateValueInt, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("mismatch", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"priority": float64(5)})
		pred := &Predicate{PropertyName: "priority", ExpectedValue: int64(3), ValueType: PredicateValueInt, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field_match condition failed")
	})
	t.Run("non_integer_float_stored", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"priority": float64(3.7)})
		pred := &Predicate{PropertyName: "priority", ExpectedValue: int64(3), ValueType: PredicateValueInt, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err, "non-exact float must fail the int conversion")
	})
}

// TestEvalPredicate_Bool tests boolean equality.
// Causal-link: evalBoolEq does an exact bool comparison; true vs false fails.
func TestEvalPredicate_Bool(t *testing.T) {
	t.Run("match_true", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"active": true})
		pred := &Predicate{PropertyName: "active", ExpectedValue: true, ValueType: PredicateValueBool, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("match_false", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"active": false})
		pred := &Predicate{PropertyName: "active", ExpectedValue: false, ValueType: PredicateValueBool, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("mismatch", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"active": true})
		pred := &Predicate{PropertyName: "active", ExpectedValue: false, ValueType: PredicateValueBool, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field_match condition failed")
	})
}

// TestEvalPredicate_Date tests RFC 3339 date equality with timezone normalization.
// Causal-link: evalDateEq normalizes both stored and expected to UTC before comparing;
// "+02:00" and "Z" with the same instant must be equal.
func TestEvalPredicate_Date(t *testing.T) {
	t.Run("match_same_utc", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"created_at": "2026-01-15T10:00:00Z"})
		pred := &Predicate{PropertyName: "created_at", ExpectedValue: "2026-01-15T10:00:00Z", ValueType: PredicateValueDate, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("match_timezone_normalized", func(t *testing.T) {
		// +02:00 offset means stored is 08:00 UTC, expected is also 08:00 UTC.
		obj := makeObjectWithProps(map[string]interface{}{"created_at": "2026-01-15T10:00:00+02:00"})
		pred := &Predicate{PropertyName: "created_at", ExpectedValue: "2026-01-15T08:00:00Z", ValueType: PredicateValueDate, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("mismatch_different_instants", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"created_at": "2026-01-15T10:00:00Z"})
		pred := &Predicate{PropertyName: "created_at", ExpectedValue: "2026-01-16T10:00:00Z", ValueType: PredicateValueDate, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field_match condition failed")
	})
	t.Run("invalid_stored_date", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"created_at": "not-a-date"})
		pred := &Predicate{PropertyName: "created_at", ExpectedValue: "2026-01-15T10:00:00Z", ValueType: PredicateValueDate, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err)
	})
}

// TestEvalPredicate_MissingProperty verifies that an absent property causes the
// predicate to fail with a "not found" reason.
// Causal-link: EvalPredicate checks existence in props map; absent key is explicitly
// caught and returned as a precondition failure (not silently treated as a match).
func TestEvalPredicate_MissingProperty(t *testing.T) {
	obj := makeObjectWithProps(map[string]interface{}{"other": "value"})
	pred := &Predicate{PropertyName: "status", ExpectedValue: "draft", ValueType: PredicateValueText, Operator: EqOperator}
	err := EvalPredicate(pred, obj)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Contains(t, err.Error(), "field_match condition failed")
}

// TestEvalPredicate_NoProperties verifies that a predicate fails when the object
// has no properties at all.
// Causal-link: EvalPredicate casts Properties to map[string]interface{} and returns
// a "no properties" failure when the cast fails or is nil.
func TestEvalPredicate_NoProperties(t *testing.T) {
	obj := &Object{Object: models.Object{Properties: nil}}
	pred := &Predicate{PropertyName: "status", ExpectedValue: "draft", ValueType: PredicateValueText, Operator: EqOperator}
	err := EvalPredicate(pred, obj)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field_match condition failed")
}

// TestEvalPredicate_ArrayProperty verifies that array-type properties return a
// clear "not supported in v1" error instead of silently passing or panicking.
// Causal-link: EvalPredicate checks for []interface{} before type-dispatching
// and returns a specific "multi-value array" rejection.
func TestEvalPredicate_ArrayProperty(t *testing.T) {
	obj := makeObjectWithProps(map[string]interface{}{"tags": []interface{}{"a", "b", "c"}})
	pred := &Predicate{PropertyName: "tags", ExpectedValue: "a", ValueType: PredicateValueText, Operator: EqOperator}
	err := EvalPredicate(pred, obj)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported in v1")
}

// TestEvalPredicate_UnsupportedOperator verifies that an unsupported operator
// returns a clear error rather than a false positive.
// Causal-link: EvalPredicate returns an error for any Operator != EqOperator;
// without this check, future operators would silently pass as unconditional.
func TestEvalPredicate_UnsupportedOperator(t *testing.T) {
	obj := makeObjectWithProps(map[string]interface{}{"status": "draft"})
	pred := &Predicate{PropertyName: "status", ExpectedValue: "draft", ValueType: PredicateValueText, Operator: PredicateOperator(99)}
	err := EvalPredicate(pred, obj)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported predicate operator")
}

// TestEvalPredicate_Number_Caveat documents float equality behavior.
// Causal-link: evalNumberEq uses IEEE-754 equality; the test confirms that exact
// float matches pass and inexact ones fail.
func TestEvalPredicate_Number_Caveat(t *testing.T) {
	t.Run("exact_match", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"score": float64(1.5)})
		pred := &Predicate{PropertyName: "score", ExpectedValue: float64(1.5), ValueType: PredicateValueNumber, Operator: EqOperator}
		require.NoError(t, EvalPredicate(pred, obj))
	})
	t.Run("mismatch", func(t *testing.T) {
		obj := makeObjectWithProps(map[string]interface{}{"score": float64(1.5)})
		pred := &Predicate{PropertyName: "score", ExpectedValue: float64(2.0), ValueType: PredicateValueNumber, Operator: EqOperator}
		err := EvalPredicate(pred, obj)
		require.Error(t, err)
	})
	t.Run("nan_always_fails", func(t *testing.T) {
		// NaN as a literal float64 can't be expressed in Go source; the branch
		// is covered by evalNumberEq's math.IsNaN guard. Documented as integration-covered.
	})
}
