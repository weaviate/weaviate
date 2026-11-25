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

package additional

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPropertiesMerge(t *testing.T) {
	// iterating the struct keeps this test failing until a newly added flag
	// is handled by Merge
	t.Run("every flag requested on either side is kept", func(t *testing.T) {
		typ := reflect.TypeOf(Properties{})
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Type.Kind() != reflect.Bool || field.Name == "NoProps" {
				continue
			}
			var set Properties
			reflect.ValueOf(&set).Elem().Field(i).SetBool(true)

			assert.True(t, reflect.ValueOf(Properties{}.Merge(set)).Field(i).Bool(), field.Name)
			assert.True(t, reflect.ValueOf(set.Merge(Properties{})).Field(i).Bool(), field.Name)
		}
	})

	t.Run("props are skipped only if neither side wants them", func(t *testing.T) {
		assert.True(t, Properties{NoProps: true}.Merge(Properties{NoProps: true}).NoProps)
		assert.False(t, Properties{NoProps: true}.Merge(Properties{}).NoProps)
		assert.False(t, Properties{}.Merge(Properties{NoProps: true}).NoProps)
	})

	t.Run("vectors are unioned", func(t *testing.T) {
		merged := Properties{Vectors: []string{"a", "b"}}.Merge(Properties{Vectors: []string{"b", "c"}})
		assert.Equal(t, []string{"a", "b", "c"}, merged.Vectors)
	})

	t.Run("module params are unioned, first one wins", func(t *testing.T) {
		first := Properties{ModuleParams: map[string]interface{}{"a": 1, "b": 1}}
		second := Properties{ModuleParams: map[string]interface{}{"b": 2, "c": 2}}

		merged := first.Merge(second)

		assert.Equal(t, map[string]interface{}{"a": 1, "b": 1, "c": 2}, merged.ModuleParams)
		assert.Equal(t, map[string]interface{}{"a": 1, "b": 1}, first.ModuleParams, "inputs are not mutated")
	})

	t.Run("nothing to merge", func(t *testing.T) {
		assert.Equal(t, Properties{}, Properties{}.Merge(Properties{}))
	})
}
