//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package runtime

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// Type represents different types that is supported in runtime configs
type DynamicType interface {
	int | float64 | bool | time.Duration
}

// Value represents any runtime config value. It's zero value is
// fully usable.
// If you want zero value with different `default`, use `NewDynamicValue` constructor.
type DynamicValue[T DynamicType] struct {
	// val is the dynamically chaning value.
	val atomic.Value
	// def represents the default value.
	def T
}

// NewDynamicValue returns an instance of DynamicValue as passed in type
// with passed in value as default.
func NewDynamicValue[T DynamicType](val T) DynamicValue[T] {
	return DynamicValue[T]{
		def: val,
	}
}

// Get returns a current value for the given config. It can either be dynamic value or default
// value (if unable to get dynamic value)
// Consumer of the dynamic config value should care only about this `Get()` api.
func (vv *DynamicValue[T]) Get() T {
	v := vv.val.Load()
	if v != nil {
		return v.(T)
	}
	return vv.def
}

// Set is used by the config manager to update the dynamic value.
func (vv *DynamicValue[T]) SetValue(val T) error {
	vv.val.Store(val)
	return nil

	// NOTE: doesn't need to set any default value here
	// as `Get()` api will return default if dynamic value is not set.
}

// SetDefault updates the `default` value for DynamicValue.
func (vv *DynamicValue[T]) SetDefault(val T) {
	vv.def = val
}

func (vv *DynamicValue[T]) UnmarshalYAML(node *yaml.Node) error {
	var zero T
	switch any(zero).(type) {
	case int:
		i, err := strconv.Atoi(node.Value)
		if err != nil {
			return fmt.Errorf("invalid int: %w", err)
		}
		vv.val.Store(i)
	case float64:
		f, err := strconv.ParseFloat(node.Value, 64)
		if err != nil {
			return fmt.Errorf("invalid float: %w", err)
		}
		vv.val.Store(f)
	case bool:
		b, err := strconv.ParseBool(node.Value)
		if err != nil {
			return fmt.Errorf("invalid bool: %w", err)
		}
		vv.val.Store(b)
	case time.Duration: // to parse time.Duration (e.g: 2m, 20s, etc)
		d, err := time.ParseDuration(node.Value)
		if err != nil {
			return fmt.Errorf("invalid duration: %w", err)
		}
		vv.val.Store(d)
	default:
		return fmt.Errorf("unsupported type in runtime config: %T", zero)
	}
	return nil
}
