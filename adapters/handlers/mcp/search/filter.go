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

package search

import (
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Filter is the LLM-facing filter DSL for MCP search tools. It collapses the
// 14 typed value fields of models.WhereFilter into a single `value`, with the
// concrete type resolved against the collection schema at translation time.
type Filter struct {
	Operator string   `json:"operator" jsonschema:"required" jsonschema_description:"And, Or, Not (combine operands); Equal, NotEqual, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual (compare); Like (text wildcard with * and ?); IsNull (value must be true or false); ContainsAny, ContainsAll, ContainsNone (value must be an array); WithinGeoRange."`
	Path     []string `json:"path,omitempty" jsonschema_description:"Property to filter on. A single name like [\"price\"], or a cross-reference path [\"inCity\",\"City\",\"name\"] (reference property, target collection, property). Required for every operator except And/Or/Not."`
	Value    any      `json:"value,omitempty" jsonschema_description:"Comparison value, matching the property's type: string for text, a number for int/number, true/false for boolean, an RFC3339 string for date. For ContainsAny/All/None pass an array. For WithinGeoRange pass {\"latitude\":<n>,\"longitude\":<n>,\"distance\":<meters>}. Omit for And/Or/Not."`
	Operands []Filter `json:"operands,omitempty" jsonschema_description:"Sub-filters combined by the And/Or/Not operator. Required for those operators, omitted otherwise."`
}

var (
	logicalOps = map[string]bool{"And": true, "Or": true, "Not": true}
	valueOps   = map[string]bool{
		"Equal": true, "NotEqual": true,
		"GreaterThan": true, "GreaterThanEqual": true,
		"LessThan": true, "LessThanEqual": true,
		"Like": true, "IsNull": true,
		"ContainsAny": true, "ContainsAll": true, "ContainsNone": true,
		"WithinGeoRange": true,
	}
	allOps = "And, Or, Not, Equal, NotEqual, GreaterThan, GreaterThanEqual, " +
		"LessThan, LessThanEqual, Like, IsNull, ContainsAny, ContainsAll, " +
		"ContainsNone, WithinGeoRange"
)

// ToWhereFilter translates the DSL into a models.WhereFilter, resolving each
// value's concrete type from the collection schema. Errors are phrased so an
// LLM caller can self-correct (valid operators, expected types, etc.).
func (f Filter) ToWhereFilter(sch schema.Schema, rootClass string) (*models.WhereFilter, error) {
	switch {
	case logicalOps[f.Operator]:
		return f.logicalToWhereFilter(sch, rootClass)
	case valueOps[f.Operator]:
		return f.valueToWhereFilter(sch, rootClass)
	default:
		return nil, fmt.Errorf("unknown filter operator %q; valid operators: %s", f.Operator, allOps)
	}
}

func (f Filter) logicalToWhereFilter(sch schema.Schema, rootClass string) (*models.WhereFilter, error) {
	if len(f.Operands) == 0 {
		return nil, fmt.Errorf("operator %q requires at least one entry in `operands`", f.Operator)
	}
	if len(f.Path) > 0 || f.Value != nil {
		return nil, fmt.Errorf("operator %q combines sub-filters; it must not set `path` or `value`", f.Operator)
	}
	out := &models.WhereFilter{Operator: f.Operator}
	for i := range f.Operands {
		child, err := f.Operands[i].ToWhereFilter(sch, rootClass)
		if err != nil {
			return nil, fmt.Errorf("operands[%d]: %w", i, err)
		}
		out.Operands = append(out.Operands, child)
	}
	return out, nil
}

func (f Filter) valueToWhereFilter(sch schema.Schema, rootClass string) (*models.WhereFilter, error) {
	if len(f.Path) == 0 {
		return nil, fmt.Errorf("operator %q requires a `path`", f.Operator)
	}
	if len(f.Operands) > 0 {
		return nil, fmt.Errorf("operator %q is a value comparison; it must not set `operands`", f.Operator)
	}
	prop := strings.Join(f.Path, ".")
	out := &models.WhereFilter{Operator: f.Operator, Path: f.Path}

	// IsNull works on any property and takes a boolean — no schema lookup needed.
	if f.Operator == "IsNull" {
		b, ok := f.Value.(bool)
		if !ok {
			return nil, fmt.Errorf("operator IsNull on %q requires a boolean value (true or false)", prop)
		}
		out.ValueBoolean = &b
		return out, nil
	}

	dt, err := resolvePropertyType(sch, rootClass, f.Path)
	if err != nil {
		return nil, err
	}

	switch f.Operator {
	case "WithinGeoRange":
		if dt != schema.DataTypeGeoCoordinates {
			return nil, fmt.Errorf("operator WithinGeoRange requires a geoCoordinates property; %q is type %s", prop, dt)
		}
		geo, err := coerceGeoRange(f.Value)
		if err != nil {
			return nil, fmt.Errorf("operator WithinGeoRange on %q: %w", prop, err)
		}
		out.ValueGeoRange = geo
	case "ContainsAny", "ContainsAll", "ContainsNone":
		if err := setArrayValue(out, dt, prop, f.Value); err != nil {
			return nil, err
		}
	default: // Equal, NotEqual, GreaterThan(Equal), LessThan(Equal), Like
		if f.Operator == "Like" && !isTextType(dt) {
			return nil, fmt.Errorf("operator Like requires a text property; %q is type %s", prop, dt)
		}
		if err := setScalarValue(out, dt, prop, f.Value); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func setScalarValue(out *models.WhereFilter, dt schema.DataType, prop string, v any) error {
	switch dt {
	case schema.DataTypeText, schema.DataTypeString, schema.DataTypeUUID, schema.DataTypeBlob:
		s, err := coerceString(v)
		if err != nil {
			return valueErr(prop, dt, err)
		}
		out.ValueText = &s
	case schema.DataTypeInt:
		n, err := coerceInt(v)
		if err != nil {
			return valueErr(prop, dt, err)
		}
		out.ValueInt = &n
	case schema.DataTypeNumber:
		n, err := coerceNumber(v)
		if err != nil {
			return valueErr(prop, dt, err)
		}
		out.ValueNumber = &n
	case schema.DataTypeBoolean:
		b, ok := v.(bool)
		if !ok {
			return valueErr(prop, dt, fmt.Errorf("expected true or false, got %s", jsonType(v)))
		}
		out.ValueBoolean = &b
	case schema.DataTypeDate:
		s, err := coerceString(v) // RFC3339 string
		if err != nil {
			return valueErr(prop, dt, err)
		}
		out.ValueDate = &s
	default:
		return fmt.Errorf("property %q has type %s, which cannot be used with a value comparison", prop, dt)
	}
	return nil
}

func setArrayValue(out *models.WhereFilter, dt schema.DataType, prop string, v any) error {
	arr, ok := v.([]any)
	if !ok {
		return fmt.Errorf("operator %s on %q requires an array value, got %s", out.Operator, prop, jsonType(v))
	}
	switch baseType(dt) {
	case schema.DataTypeText, schema.DataTypeString, schema.DataTypeUUID:
		for _, e := range arr {
			s, err := coerceString(e)
			if err != nil {
				return valueErr(prop, dt, err)
			}
			out.ValueTextArray = append(out.ValueTextArray, s)
		}
	case schema.DataTypeInt:
		for _, e := range arr {
			n, err := coerceInt(e)
			if err != nil {
				return valueErr(prop, dt, err)
			}
			out.ValueIntArray = append(out.ValueIntArray, n)
		}
	case schema.DataTypeNumber:
		for _, e := range arr {
			n, err := coerceNumber(e)
			if err != nil {
				return valueErr(prop, dt, err)
			}
			out.ValueNumberArray = append(out.ValueNumberArray, n)
		}
	case schema.DataTypeBoolean:
		for _, e := range arr {
			b, ok := e.(bool)
			if !ok {
				return valueErr(prop, dt, fmt.Errorf("expected true or false, got %s", jsonType(e)))
			}
			out.ValueBooleanArray = append(out.ValueBooleanArray, b)
		}
	case schema.DataTypeDate:
		for _, e := range arr {
			s, err := coerceString(e)
			if err != nil {
				return valueErr(prop, dt, err)
			}
			out.ValueDateArray = append(out.ValueDateArray, s)
		}
	default:
		return fmt.Errorf("property %q has type %s, which cannot be used with %s", prop, dt, out.Operator)
	}
	return nil
}

// resolvePropertyType walks a (possibly cross-reference) path and returns the
// data type of the final property. Path layout: [prop] for a direct property,
// or [refProp, TargetClass, prop, ...] for one or more reference hops.
func resolvePropertyType(sch schema.Schema, rootClass string, path []string) (schema.DataType, error) {
	current := rootClass
	rest := path
	for len(rest) > 1 {
		refProp, target := rest[0], rest[1]
		p, err := findProperty(sch, current, refProp)
		if err != nil {
			return "", err
		}
		if !isCrossRef(p) {
			return "", fmt.Errorf("property %q on %q is not a cross-reference and cannot be a path segment", refProp, current)
		}
		found := false
		for _, dt := range p.DataType {
			if dt == target {
				found = true
				break
			}
		}
		if !found {
			return "", fmt.Errorf("reference %q does not point to collection %q (it points to: %s)",
				refProp, target, strings.Join(p.DataType, ", "))
		}
		current, rest = target, rest[2:]
	}
	p, err := findProperty(sch, current, rest[0])
	if err != nil {
		return "", err
	}
	if len(p.DataType) == 0 {
		return "", fmt.Errorf("property %q on %q has no declared data type", rest[0], current)
	}
	return schema.DataType(p.DataType[0]), nil
}

func findProperty(sch schema.Schema, className, propName string) (*models.Property, error) {
	cls := sch.GetClass(className)
	if cls == nil {
		return nil, fmt.Errorf("collection %q not found in schema", className)
	}
	names := make([]string, 0, len(cls.Properties))
	for _, p := range cls.Properties {
		if p.Name == propName {
			return p, nil
		}
		names = append(names, p.Name)
	}
	return nil, fmt.Errorf("property %q not found on collection %q; available properties: %s",
		propName, className, strings.Join(names, ", "))
}

// isCrossRef reports whether a property is a reference. Reference properties
// carry capitalised target-class names; primitive types (text, int, ...) are
// lower-case.
func isCrossRef(p *models.Property) bool {
	return len(p.DataType) > 0 && p.DataType[0] != "" && unicode.IsUpper(rune(p.DataType[0][0]))
}

func baseType(dt schema.DataType) schema.DataType {
	return schema.DataType(strings.TrimSuffix(string(dt), "[]"))
}

func isTextType(dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeText, schema.DataTypeString, schema.DataTypeUUID:
		return true
	default:
		return false
	}
}

func valueErr(prop string, dt schema.DataType, err error) error {
	return fmt.Errorf("value for property %q (type %s): %w", prop, dt, err)
}

// JSON decoding yields float64 for every number; the helpers below also accept
// native Go integers so the DSL is usable from Go callers, not just JSON. They
// give typed values with clear errors an LLM caller can act on.

func coerceString(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected a string, got %s", jsonType(v))
	}
	return s, nil
}

func coerceNumber(v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("expected a number, got %s", jsonType(v))
	}
}

func coerceInt(v any) (int64, error) {
	switch n := v.(type) {
	case float64:
		if n != math.Trunc(n) {
			return 0, fmt.Errorf("expected an integer, got %v", n)
		}
		return int64(n), nil
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	default:
		return 0, fmt.Errorf("expected an integer, got %s", jsonType(v))
	}
}

func coerceGeoRange(v any) (*models.WhereFilterGeoRange, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected an object {latitude, longitude, distance}, got %s", jsonType(v))
	}
	lat, err := coerceNumber(m["latitude"])
	if err != nil {
		return nil, fmt.Errorf("latitude: %w", err)
	}
	lon, err := coerceNumber(m["longitude"])
	if err != nil {
		return nil, fmt.Errorf("longitude: %w", err)
	}
	dist, err := coerceNumber(m["distance"])
	if err != nil {
		return nil, fmt.Errorf("distance: %w", err)
	}
	lat32, lon32 := float32(lat), float32(lon)
	return &models.WhereFilterGeoRange{
		GeoCoordinates: &models.GeoCoordinates{Latitude: &lat32, Longitude: &lon32},
		Distance:       &models.WhereFilterGeoRangeDistance{Max: dist},
	}, nil
}

func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}
