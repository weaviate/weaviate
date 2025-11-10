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
        if t < graphqlIntMin || t > graphqlIntMax {
            return 0, fmt.Errorf("int64 out of GraphQL Int range: %d", t)
        }
        return int(t), nil
    case float64:
        if math.Trunc(t) != t {
            return 0, fmt.Errorf("float not integral: %v", t)
        }
        if t < float64(graphqlIntMin) || t > float64(graphqlIntMax) {
            return 0, fmt.Errorf("float out of GraphQL Int range: %v", t)
        }
        return int(t), nil
    case json.Number:
        if i64, err := t.Int64(); err == nil {
            if i64 < graphqlIntMin || i64 > graphqlIntMax {
                return 0, fmt.Errorf("json.Number out of GraphQL Int range: %v", t)
            }
            return int(i64), nil
        }
        // fallback to float check
        if f, err := t.Float64(); err == nil {
            if math.Trunc(f) != f {
                return 0, fmt.Errorf("json.Number not integral: %v", t)
            }
            if f < float64(graphqlIntMin) || f > float64(graphqlIntMax) {
                return 0, fmt.Errorf("json.Number out of GraphQL Int range: %v", t)
            }
            return int(f), nil
        }
        return 0, fmt.Errorf("invalid json.Number: %v", t)
    case string:
        if i64, err := strconv.ParseInt(t, 10, 64); err == nil {
            if i64 < graphqlIntMin || i64 > graphqlIntMax {
                return 0, fmt.Errorf("string int out of GraphQL Int range: %q", t)
            }
            return int(i64), nil
        }
        if f, err := strconv.ParseFloat(t, 64); err == nil {
            if math.Trunc(f) != f {
                return 0, fmt.Errorf("string float not integral: %q", t)
            }
            if f < float64(graphqlIntMin) || f > float64(graphqlIntMax) {
                return 0, fmt.Errorf("string float out of GraphQL Int range: %q", t)
            }
            return int(f), nil
        }
        return 0, fmt.Errorf("string not numeric: %q", t)
    default:
        return 0, fmt.Errorf("unsupported type %T for GraphQL Int", v)
    }
}
