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

package config

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"gopkg.in/yaml.v3"
)

// WeaviateRuntimeConfig is the collection all the supported configs that is
// managed dynamically and can be overridden during runtime.
type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionsCount       *runtime.DynamicValue[int]           `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`
	AutoschemaEnabled                    *runtime.DynamicValue[bool]          `json:"autoschema_enabled" yaml:"autoschema_enabled"`
	AsyncReplicationDisabled             *runtime.DynamicValue[bool]          `json:"async_replication_disabled" yaml:"async_replication_disabled"`
	ReplicaMovementMinimumFinalizingWait *runtime.DynamicValue[time.Duration] `json:"replica_movement_minimum_finalizing_wait" yaml:"replica_movement_minimum_finalizing_wait"`
}

// ParseRuntimeConfig decode WeaviateRuntimeConfig from raw bytes of YAML.
func ParseRuntimeConfig(buf []byte) (*WeaviateRuntimeConfig, error) {
	var conf WeaviateRuntimeConfig

	dec := yaml.NewDecoder(bytes.NewReader(buf))

	// To catch fields different than ones in the struct (say typo)
	dec.KnownFields(true)

	// Am empty runtime yaml file is still a valid file. So treating io.EOF as
	// non-error case returns default values of config.
	if err := dec.Decode(&conf); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &conf, nil
}

// UpdateConfig does in-place update of `source` config based on values available in
// `parsed` config.
func UpdateRuntimeConfig(source, parsed *WeaviateRuntimeConfig) error {
	if source == nil || parsed == nil {
		return fmt.Errorf("source and parsed cannot be nil")
	}

	updateRuntimeConfig(reflect.ValueOf(*source), reflect.ValueOf(*parsed))
	return nil
}

/*
Alright. `updateRuntimeConfig` needs some explanation.

We could have avoided using `reflection` all together, if we have written something like this.

	func updateRuntimeConfig(source, parsed *WeaviateRuntimeConfig) error {
		if parsed.MaximumAllowedCollectionsCount != nil {
			source.MaximumAllowedCollectionsCount.SetValue(parsed.MaximumAllowedCollectionsCount.Get())
		} else {
			source.MaximumAllowedCollectionsCount.Reset()
		}

		if parsed.AsyncReplicationDisabled != nil {
			source.AsyncReplicationDisabled.SetValue(parsed.AsyncReplicationDisabled.Get())
		} else {
			source.AsyncReplicationDisabled.Reset()
		}

		if parsed.AutoschemaEnabled != nil {
			source.AutoschemaEnabled.SetValue(parsed.AutoschemaEnabled.Get())
		} else {
			source.AutoschemaEnabled.Reset()
		}

		return nil
	}

But this approach has two serious drawbacks
 1. Everytime new config is supported, this function gets verbose as we have update for every struct fields in WeaviateRuntimeConfig
 2. The much bigger one is, what if consumer added a struct field, but failed to **update** this function?. This was a serious concern for me, more work for
    consumers.

With this reflection method, we avoided that extra step from the consumer. This reflection approach is "logically" same as above implementation.
See "runtimeconfig_test.go" for more examples.
*/
func updateRuntimeConfig(source, parsed reflect.Value) {
	// Basically we do following
	//
	// 1. Loop through all the `source` fields
	// 2. Check if any of those fields exists in `parsed` (non-nil)
	// 3. If parsed config doesn't contain the field from `source`, We reset source's field.
	//    so that it's default value takes preference.
	// 4. If parsed config does contain the field from `source`, We update the value via `SetValue`.

	for i := range source.NumField() {
		sf := source.Field(i)
		pf := parsed.Field(i)

		if pf.IsNil() {
			si := sf.Interface()
			src, ok := si.(interface{ Reset() })
			if !ok {
				panic(fmt.Sprintf("type doesn't have Reset() method: %#v", si))
			}
			src.Reset()
			continue
		}

		si := sf.Interface()
		pi := pf.Interface()

		switch sv := si.(type) {
		case *runtime.DynamicValue[int]:
			// this casting is fine, because both `parsed` and `source` are same struct.
			p := pi.(*runtime.DynamicValue[int])
			sv.SetValue(p.Get())
		case *runtime.DynamicValue[float64]:
			p := pi.(*runtime.DynamicValue[float64])
			sv.SetValue(p.Get())
		case *runtime.DynamicValue[bool]:
			p := pi.(*runtime.DynamicValue[bool])
			sv.SetValue(p.Get())
		case *runtime.DynamicValue[time.Duration]:
			p := pi.(*runtime.DynamicValue[time.Duration])
			sv.SetValue(p.Get())
		case *runtime.DynamicValue[string]:
			p := pi.(*runtime.DynamicValue[string])
			sv.SetValue(p.Get())
		default:
			panic(fmt.Sprintf("not recognized type: %#v, %#v", pi, si))
		}

	}
}
