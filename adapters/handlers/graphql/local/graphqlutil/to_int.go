package graphqlutil

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

const (
	graphqlIntMin = -1 << 31
	graphqlIntMax = 1<<31 - 1
)

// ToInt coerces common JSON/Go types into a GraphQL Int (Go int).
// Accepts int, int32, int64, float64 (only integral), json.Number, and numeric strings.
// Returns error if value is non-integral or outside GraphQL 32-bit Int range.
func ToInt(v interface{}) (int, error) {
	switch t := v.(type) {
	case int:
		return t, nil
	case int32:
		return int(t), nil
	case int64:
		return fromInt64(t)
	case float64:
		return fromFloat64(t)
	case json.Number:
		return fromJSONNumber(t)
	case string:
		return fromString(t)
	default:
		return 0, fmt.Errorf("unsupported type %T for GraphQL Int", v)
	}
}

func fromInt64(v int64) (int, error) {
	if err := ensureInt64Range(v); err != nil {
		return 0, err
	}
	return int(v), nil
}

func fromFloat64(v float64) (int, error) {
	if err := ensureIntegralFloat(v); err != nil {
		return 0, err
	}
	if err := ensureFloat64Range(v); err != nil {
		return 0, err
	}
	return int(v), nil
}

func fromJSONNumber(v json.Number) (int, error) {
	if i64, err := v.Int64(); err == nil {
		return fromInt64(i64)
	}
	f, err := v.Float64()
	if err != nil {
		return 0, fmt.Errorf("invalid json.Number: %v", v)
	}
	return fromFloat64(f)
}

func fromString(v string) (int, error) {
	if i64, err := strconv.ParseInt(v, 10, 64); err == nil {
		return fromInt64(i64)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("string not numeric: %q", v)
	}
	return fromFloat64(f)
}

func ensureInt64Range(v int64) error {
	if v < graphqlIntMin || v > graphqlIntMax {
		return fmt.Errorf("int64 out of GraphQL Int range: %d", v)
	}
	return nil
}

func ensureFloat64Range(v float64) error {
	if v < float64(graphqlIntMin) || v > float64(graphqlIntMax) {
		return fmt.Errorf("float out of GraphQL Int range: %v", v)
	}
	return nil
}

func ensureIntegralFloat(v float64) error {
	if math.Trunc(v) != v {
		return fmt.Errorf("float not integral: %v", v)
	}
	return nil
}
