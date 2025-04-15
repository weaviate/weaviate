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
	"errors"
	"sync/atomic"
	"time"
)

const (
	// These are currently supported runtime configs
	MaximumAllowedCollectionCount = "maximum_allowed_collections_count"
	AutoschemaEnabled             = "autoschema_enabled"
	AsyncReplicationDisabled      = "async_replication_disabled"
)

// Type represents different types that is supported in runtime configs
type DynamicType interface {
	int | float64 | bool | time.Duration
}

// Value represents any runtime config value.
type DynamicValue[T DynamicType] struct {
	// val is the dynamically chaning value.
	val atomic.Value
	// def represents the default value.
	def T
}

// NewDynamicValue returns an instance of DynamicValue as passed in type
// with passed in value as default.
func NewDynamicValue[T DynamicType](val T) *DynamicValue[T] {
	dv := &DynamicValue[T]{
		def: val,
	}
	dv.val.Store(val)
	return dv
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

// NOTE: Set accepts `any` so that config manager can use this API to maintain the `map` of
// registered dynamic configs. Go needs concrete type in map, we cannot use
// generic types like DynamicType or DynamicValue there. Hence `any`( ¯\_(ツ)_/¯)
func (vv *DynamicValue[T]) SetValue(val any) error {
	v, ok := val.(T)
	if !ok {
		return errors.New("mismatched type")
	}

	vv.val.Store(v)
	return nil

	// NOTE: doesn't need to set any default value here
	// as `Get()` api will return default if dynamic value is not set.
}

// SetDefault updates the `default` value for DynamicValue.
func (vv *DynamicValue[T]) SetDefault(val T) {
	vv.def = val
}
