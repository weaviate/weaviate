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
)

// TestParsePredicateFromQueryParams_AllAbsent verifies that absent params → nil, nil.
// Causal-link: ParsePredicateFromQueryParams returns nil when all three params are
// empty, enabling the "no predicate" path without allocating a Predicate.
func TestParsePredicateFromQueryParams_AllAbsent(t *testing.T) {
	pred, err := ParsePredicateFromQueryParams("", "", "")
	require.NoError(t, err)
	assert.Nil(t, pred, "all-absent params must return nil predicate (unconditional write)")
}

// TestParsePredicateFromQueryParams_Text tests text parsing round-trip.
// Causal-link: ParsePredicateFromQueryParams with value_type=text must set
// PredicateValueText and store the raw string in ExpectedValue; SerializePredicate
// must reproduce the original three params.
func TestParsePredicateFromQueryParams_Text(t *testing.T) {
	pred, err := ParsePredicateFromQueryParams("status", "draft", "text")
	require.NoError(t, err)
	require.NotNil(t, pred)
	assert.Equal(t, "status", pred.PropertyName)
	assert.Equal(t, "draft", pred.ExpectedValue)
	assert.Equal(t, PredicateValueText, pred.ValueType)
	assert.Equal(t, EqOperator, pred.Operator)

	// Round-trip.
	prop, val, vtype := SerializePredicateToQueryParams(pred)
	assert.Equal(t, "status", prop)
	assert.Equal(t, "draft", val)
	assert.Equal(t, "text", vtype)
}

// TestParsePredicateFromQueryParams_Int tests int64 parsing and round-trip.
// Causal-link: ParsePredicateFromQueryParams must parse a decimal int64 from the
// value string; SerializePredicateToQueryParams must reproduce it as a decimal string.
func TestParsePredicateFromQueryParams_Int(t *testing.T) {
	pred, err := ParsePredicateFromQueryParams("priority", "42", "int")
	require.NoError(t, err)
	require.NotNil(t, pred)
	assert.Equal(t, int64(42), pred.ExpectedValue)
	assert.Equal(t, PredicateValueInt, pred.ValueType)

	prop, val, vtype := SerializePredicateToQueryParams(pred)
	assert.Equal(t, "priority", prop)
	assert.Equal(t, "42", val)
	assert.Equal(t, "int", vtype)
}

// TestParsePredicateFromQueryParams_Bool tests bool parsing.
// Causal-link: ParsePredicateFromQueryParams must parse "true"/"false" to Go bool.
func TestParsePredicateFromQueryParams_Bool(t *testing.T) {
	for _, s := range []string{"true", "True", "TRUE"} {
		pred, err := ParsePredicateFromQueryParams("active", s, "bool")
		require.NoError(t, err, "input %q", s)
		require.NotNil(t, pred)
		assert.Equal(t, true, pred.ExpectedValue, "input %q must parse to true", s)
	}

	pred, err := ParsePredicateFromQueryParams("active", "false", "bool")
	require.NoError(t, err)
	assert.Equal(t, false, pred.ExpectedValue)
}

// TestParsePredicateFromQueryParams_Date tests date type (stored as string).
// Causal-link: Date values are stored as-is in ExpectedValue; EvalPredicate parses them.
func TestParsePredicateFromQueryParams_Date(t *testing.T) {
	pred, err := ParsePredicateFromQueryParams("published_at", "2026-01-15T10:00:00Z", "date")
	require.NoError(t, err)
	require.NotNil(t, pred)
	assert.Equal(t, "2026-01-15T10:00:00Z", pred.ExpectedValue)
	assert.Equal(t, PredicateValueDate, pred.ValueType)
}

// TestParsePredicateFromQueryParams_InvalidInt verifies that a non-numeric value for
// value_type=int returns an error.
// Causal-link: strconv.ParseInt fails on "notanumber" and the error is propagated.
func TestParsePredicateFromQueryParams_InvalidInt(t *testing.T) {
	_, err := ParsePredicateFromQueryParams("priority", "notanumber", "int")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid int value")
}

// TestParsePredicateFromQueryParams_UnknownType verifies that an unknown value_type
// returns a clear error.
// Causal-link: PredicateValueTypeByName lookup fails on unknown keys.
func TestParsePredicateFromQueryParams_UnknownType(t *testing.T) {
	_, err := ParsePredicateFromQueryParams("x", "v", "jsonb")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown value_type")
}

// TestParsePredicateFromQueryParams_PartialParams verifies that partial presence
// (some but not all params) returns an error.
// Causal-link: The protocol requires all three params; partial presence is a client error.
func TestParsePredicateFromQueryParams_PartialParams(t *testing.T) {
	_, err := ParsePredicateFromQueryParams("status", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conditional_field_property")

	_, err = ParsePredicateFromQueryParams("", "draft", "text")
	require.Error(t, err)
}

// TestSerializePredicateToQueryParams_Nil verifies that nil predicate returns empty strings.
// Causal-link: Nil is the "no predicate" sentinel; serialization must return empty to
// avoid setting spurious query params on the replication wire.
func TestSerializePredicateToQueryParams_Nil(t *testing.T) {
	prop, val, vtype := SerializePredicateToQueryParams(nil)
	assert.Equal(t, "", prop)
	assert.Equal(t, "", val)
	assert.Equal(t, "", vtype)
}

// TestParsePredicateRoundTrip_AllTypes verifies serialize→parse round-trip for all
// supported value types, including the empty-string text case.
// Causal-link: The replication client serializes and the receiver parses; any loss of
// information in this round-trip would silently change the predicate semantics.
// The empty-string text case specifically catches the bug where value=="" was used
// as the "predicate absent" sentinel, causing empty-string predicates to be dropped.
func TestParsePredicateRoundTrip_AllTypes(t *testing.T) {
	cases := []struct {
		name string
		pred *Predicate
	}{
		{"text", &Predicate{PropertyName: "s", ExpectedValue: "hello world", ValueType: PredicateValueText, Operator: EqOperator}},
		{"text_empty_value", &Predicate{PropertyName: "status", ExpectedValue: "", ValueType: PredicateValueText, Operator: EqOperator}},
		{"int", &Predicate{PropertyName: "n", ExpectedValue: int64(-99), ValueType: PredicateValueInt, Operator: EqOperator}},
		{"int_zero", &Predicate{PropertyName: "n", ExpectedValue: int64(0), ValueType: PredicateValueInt, Operator: EqOperator}},
		{"number", &Predicate{PropertyName: "f", ExpectedValue: float64(3.14), ValueType: PredicateValueNumber, Operator: EqOperator}},
		{"bool_true", &Predicate{PropertyName: "b", ExpectedValue: true, ValueType: PredicateValueBool, Operator: EqOperator}},
		{"bool_false", &Predicate{PropertyName: "b", ExpectedValue: false, ValueType: PredicateValueBool, Operator: EqOperator}},
		{"date", &Predicate{PropertyName: "d", ExpectedValue: "2026-06-01T00:00:00Z", ValueType: PredicateValueDate, Operator: EqOperator}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prop, val, vtype := SerializePredicateToQueryParams(tc.pred)
			parsed, err := ParsePredicateFromQueryParams(prop, val, vtype)
			require.NoError(t, err, "round-trip must not error for %s", tc.name)
			require.NotNil(t, parsed, "round-trip must not return nil predicate for %s", tc.name)
			assert.Equal(t, tc.pred.PropertyName, parsed.PropertyName)
			assert.Equal(t, tc.pred.ExpectedValue, parsed.ExpectedValue)
			assert.Equal(t, tc.pred.ValueType, parsed.ValueType)
			assert.Equal(t, tc.pred.Operator, parsed.Operator)
		})
	}
}

// TestParsePredicateFromQueryParams_EmptyStringValue verifies that value="" is a valid
// text predicate (not a "predicate absent" sentinel) when property and value_type are set.
// Causal-link: This test catches the bug where ParsePredicateFromQueryParams treated
// value=="" as partial-presence (returning an error), making "update only if field equals
// empty string" inexpressible over the REST/replication wire. With the fix, presence is
// determined by property+value_type, allowing value="" for text predicates.
func TestParsePredicateFromQueryParams_EmptyStringValue(t *testing.T) {
	pred, err := ParsePredicateFromQueryParams("status", "", "text")
	require.NoError(t, err, "empty string value with value_type=text must be accepted")
	require.NotNil(t, pred, "empty string value with value_type=text must return a non-nil predicate")
	assert.Equal(t, "status", pred.PropertyName)
	assert.Equal(t, "", pred.ExpectedValue)
	assert.Equal(t, PredicateValueText, pred.ValueType)
	assert.Equal(t, EqOperator, pred.Operator)

	// Confirm the round-trip also preserves the empty string.
	prop, val, vtype := SerializePredicateToQueryParams(pred)
	assert.Equal(t, "status", prop)
	assert.Equal(t, "", val)
	assert.Equal(t, "text", vtype)

	reparsed, err := ParsePredicateFromQueryParams(prop, val, vtype)
	require.NoError(t, err, "serialize→parse round-trip of empty-string predicate must not error")
	require.NotNil(t, reparsed)
	assert.Equal(t, "", reparsed.ExpectedValue, "empty-string value must survive the wire round-trip")
}

// TestParsePredicateFromQueryParams_PartialPresenceStillErrors verifies that partial
// presence (property or value_type missing while the other is set) still produces a
// protocol error, regardless of whether value is empty or not.
// Causal-link: The new presence discriminator (property+value_type) must still reject
// malformed requests where either the property name or the value_type is absent.
func TestParsePredicateFromQueryParams_PartialPresenceStillErrors(t *testing.T) {
	cases := []struct {
		name      string
		property  string
		value     string
		valueType string
	}{
		{"missing_value_type_nonempty_value", "status", "draft", ""},
		{"missing_value_type_empty_value", "status", "", ""},
		{"missing_property_nonempty_value", "", "draft", "text"},
		{"only_value_set", "", "draft", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePredicateFromQueryParams(tc.property, tc.value, tc.valueType)
			require.Error(t, err, "partial presence (%q,%q,%q) must return an error", tc.property, tc.value, tc.valueType)
			assert.Contains(t, err.Error(), "conditional_field_property")
		})
	}
}
