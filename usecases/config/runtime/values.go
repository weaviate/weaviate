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
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// Type represents different types that is supported in runtime configs
type DynamicType interface {
	~int | ~float64 | ~bool | time.Duration | ~string
}

// Value represents any runtime config value. It's zero value is
// fully usable.
// If you want zero value with different `default`, use `NewDynamicValue` constructor.
type DynamicValue[T DynamicType] struct {
	// val is the dynamically changing value.
	val atomic.Value
	// protects `val` during Reset()
	valMu sync.Mutex

	// def represents the default value.
	def T
	// protects `def`
	defMu sync.Mutex
}

// NewDynamicValue returns an instance of DynamicValue as passed in type
// with passed in value as default.
func NewDynamicValue[T DynamicType](val T) *DynamicValue[T] {
	return &DynamicValue[T]{
		def: val,
	}
}

// Get returns a current value for the given config. It can either be dynamic value or default
// value (if unable to get dynamic value)
// Consumer of the dynamic config value should care only about this `Get()` api.
func (dv *DynamicValue[T]) Get() T {
	// Handle zero-value of `*DynamicValue[T]` without panic.
	if dv == nil {
		var zero T
		return zero
	}

	v := dv.val.Load()
	if v != nil {
		return v.(T)
	}

	dv.defMu.Lock()
	def := dv.def
	dv.defMu.Unlock()

	return def
}

// Reset removes the old dynamic value.
func (dv *DynamicValue[T]) Reset() {
	// Once atomic value is set via `Store()`, no api to reset it
	// hence replacing the full value.
	dv.valMu.Lock()
	defer dv.valMu.Unlock()

	dv.val = atomic.Value{}
}

// Set is used by the config manager to update the dynamic value.
func (dv *DynamicValue[T]) SetValue(val T) {
	dv.val.Store(val)

	// NOTE: doesn't need to set any default value here
	// as `Get()` api will return default if dynamic value is not set.
}

// SetDefault updates the `default` value for DynamicValue.
func (dv *DynamicValue[T]) SetDefault(val T) {
	dv.defMu.Lock()
	defer dv.defMu.Unlock()

	dv.def = val
}

// UnmarshalYAML implements `yaml.v3` custom decoding for `DynamicValue` type.
func (dv *DynamicValue[T]) UnmarshalYAML(node *yaml.Node) error {
	var zero T
	switch any(zero).(type) {
	case int:
		i, err := strconv.Atoi(node.Value)
		if err != nil {
			return fmt.Errorf("invalid int: %w", err)
		}
		dv.val.Store(i)
	case float64:
		f, err := strconv.ParseFloat(node.Value, 64)
		if err != nil {
			return fmt.Errorf("invalid float: %w", err)
		}
		dv.val.Store(f)
	case bool:
		b, err := strconv.ParseBool(node.Value)
		if err != nil {
			return fmt.Errorf("invalid bool: %w", err)
		}
		dv.val.Store(b)
	case time.Duration: // to parse time.Duration (e.g: 2m, 20s, etc)
		d, err := time.ParseDuration(node.Value)
		if err != nil {
			return fmt.Errorf("invalid duration: %w", err)
		}
		dv.val.Store(d)
	case string:
		dv.val.Store(node.Value)
	default:
		return fmt.Errorf("unsupported type in runtime config: %T", zero)
	}
	return nil
}
