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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// DynamicType represents different types that is supported in runtime configs
type DynamicType interface {
	~int | ~float64 | ~bool | time.Duration | ~string | []string
}

// DynamicValue represents any runtime config value. It's zero value is
// fully usable.
// If you want zero value with different `default`, use `NewDynamicValue` constructor.
type DynamicValue[T DynamicType] struct {
	// val is the dynamically changing value.
	val *T
	// mu protects val
	mu sync.RWMutex

	// def represents the default value.
	def T
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

	dv.mu.RLock()
	defer dv.mu.RUnlock()

	if dv.val != nil {
		return *dv.val
	}
	return dv.def
}

// Reset removes the old dynamic value.
func (dv *DynamicValue[T]) Reset() {
	if dv == nil {
		return
	}

	dv.mu.Lock()
	defer dv.mu.Unlock()

	dv.val = nil
}

// Set is used by the config manager to update the dynamic value.
func (dv *DynamicValue[T]) SetValue(val T) {
	// log this at the high level, that someone is trying to set unitilized runtime value
	if dv == nil {
		return
	}

	dv.mu.Lock()
	defer dv.mu.Unlock()

	dv.val = &val

	// NOTE: doesn't need to set any default value here
	// as `Get()` api will return default if dynamic value is not set.
}

// UnmarshalYAML implements `yaml.v3` custom decoding for `DynamicValue` type.
func (dv *DynamicValue[T]) UnmarshalYAML(node *yaml.Node) error {
	var val T
	if err := node.Decode(&val); err != nil {
		return err
	}
	dv.mu.Lock()
	defer dv.mu.Unlock()

	dv.def = val
	return nil
}

// MarshalYAML implements `yaml.v3` custom encoding for `DynamicValue` type.
func (dv *DynamicValue[T]) MarshalYAML() (any, error) {
	dv.mu.Lock()
	val := dv.def
	if dv.val != nil {
		val = *dv.val
	}
	dv.mu.Unlock()

	return val, nil
}

// UnmarshalJSON implements `json` custom decoding for `DynamicValue` type.
func (dv *DynamicValue[T]) UnmarshalJSON(data []byte) error {
	var val T
	if err := json.Unmarshal(data, &val); err != nil {
		return err
	}
	dv.mu.Lock()
	defer dv.mu.Unlock()
	dv.def = val
	return nil
}

// MarshalJSON implements `json` custom encoding for `DynamicValue` type.
func (dv *DynamicValue[T]) MarshalJSON() ([]byte, error) {
	dv.mu.Lock()
	val := dv.def
	if dv.val != nil {
		val = *dv.val
	}
	dv.mu.Unlock()

	b, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// String implements Stringer interface for `%v` formatting
// for any dynamic value types.
func (dv *DynamicValue[T]) String() string {
	res := dv.val
	if res == nil {
		res = &dv.def
	}
	return fmt.Sprintf("%v", *res)
}
