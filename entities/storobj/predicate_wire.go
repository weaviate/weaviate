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
	"strconv"
	"strings"
)

// PredicateValueTypeName maps PredicateValueType to its canonical wire-string.
// Used by REST and replication-client to serialize/deserialize the value_type
// query parameter.
var PredicateValueTypeName = map[PredicateValueType]string{
	PredicateValueText:   "text",
	PredicateValueInt:    "int",
	PredicateValueNumber: "number",
	PredicateValueBool:   "bool",
	PredicateValueDate:   "date",
}

// PredicateValueTypeByName is the inverse of PredicateValueTypeName.
var PredicateValueTypeByName = func() map[string]PredicateValueType {
	m := make(map[string]PredicateValueType, len(PredicateValueTypeName))
	for k, v := range PredicateValueTypeName {
		m[v] = k
	}
	return m
}()

// ParsePredicateFromQueryParams builds a *Predicate from the three replication-wire
// query params: property name, value string, and value type name. Returns nil, nil
// when all three are absent (no field-predicate condition). Returns an error if
// any are present but invalid or incomplete.
//
// The value string is decoded per the value type:
//   - text: raw string
//   - int:  decimal int64
//   - number: decimal float64
//   - bool: "true" or "false" (case-insensitive)
//   - date: RFC 3339 string (stored as-is; EvalPredicate parses it)
func ParsePredicateFromQueryParams(property, valueStr, valueTypeName string) (*Predicate, error) {
	// All three absent = no condition.
	if property == "" && valueStr == "" && valueTypeName == "" {
		return nil, nil
	}
	// Partial presence is a protocol error.
	if property == "" || valueStr == "" || valueTypeName == "" {
		return nil, fmt.Errorf("field_match condition requires all three params: conditional_field_property, conditional_field_value, conditional_field_value_type (got property=%q value=%q value_type=%q)", property, valueStr, valueTypeName)
	}

	vt, ok := PredicateValueTypeByName[strings.ToLower(valueTypeName)]
	if !ok {
		return nil, fmt.Errorf("unknown value_type %q for field_match; supported: text, int, number, bool, date", valueTypeName)
	}

	var expected interface{}
	switch vt {
	case PredicateValueText, PredicateValueDate:
		expected = valueStr
	case PredicateValueInt:
		v, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid int value %q for value_type=int in field_match: %w", valueStr, err)
		}
		expected = v
	case PredicateValueNumber:
		v, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number value %q for value_type=number in field_match: %w", valueStr, err)
		}
		expected = v
	case PredicateValueBool:
		v, err := strconv.ParseBool(valueStr)
		if err != nil {
			return nil, fmt.Errorf("invalid bool value %q for value_type=bool in field_match; use 'true' or 'false': %w", valueStr, err)
		}
		expected = v
	}

	return &Predicate{
		PropertyName:  property,
		ExpectedValue: expected,
		ValueType:     vt,
		Operator:      EqOperator,
	}, nil
}

// SerializePredicateToQueryParams returns the three wire-param values for p.
// The caller sets them as query parameters named ConditionalFieldPropertyKey,
// ConditionalFieldValueKey, ConditionalFieldValueTypeKey.
// Returns empty strings when p is nil.
func SerializePredicateToQueryParams(p *Predicate) (property, valueStr, valueTypeName string) {
	if p == nil {
		return "", "", ""
	}
	property = p.PropertyName
	valueTypeName = PredicateValueTypeName[p.ValueType]
	switch p.ValueType {
	case PredicateValueText, PredicateValueDate:
		if s, ok := p.ExpectedValue.(string); ok {
			valueStr = s
		}
	case PredicateValueInt:
		if v, ok := p.ExpectedValue.(int64); ok {
			valueStr = strconv.FormatInt(v, 10)
		}
	case PredicateValueNumber:
		if v, ok := p.ExpectedValue.(float64); ok {
			valueStr = strconv.FormatFloat(v, 'g', -1, 64)
		}
	case PredicateValueBool:
		if v, ok := p.ExpectedValue.(bool); ok {
			valueStr = strconv.FormatBool(v)
		}
	}
	return property, valueStr, valueTypeName
}
