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

package moduletools

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectDiff(t *testing.T) {
	t.Run("strings are the same", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("sameStrings", "Some string", "Some string")

		assert.False(t, objDiff.IsChangedProp("sameStrings"))
	})

	t.Run("strings are different", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("differentStrings1", "Some string", "some string").
			WithProp("differentStrings2", "Some string", "Some string, but different")

		assert.True(t, objDiff.IsChangedProp("differentStrings1"))
		assert.True(t, objDiff.IsChangedProp("differentStrings2"))
	})

	t.Run("string slices are the same", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("sameStringSlices1", []string{"aa", "bb", "cc"}, []string{"aa", "bb", "cc"}).
			WithProp("sameStringSlices2", []string{"aa", "bb", "cc"}, []interface{}{"aa", "bb", "cc"}).
			WithProp("sameStringSlices3", []interface{}{"aa", "bb", "cc"}, []string{"aa", "bb", "cc"}).
			WithProp("sameStringSlices4", []interface{}{"aa", "bb", "cc"}, []interface{}{"aa", "bb", "cc"}).
			WithProp("sameStringSlices5", []string{}, []string{}).
			WithProp("sameStringSlices6", []string{}, []interface{}{}).
			WithProp("sameStringSlices7", []interface{}{}, []string{}).
			WithProp("sameStringSlices8", []interface{}{}, []interface{}{})

		for _, prop := range []string{
			"sameStringSlices1", "sameStringSlices2", "sameStringSlices3", "sameStringSlices4",
			"sameStringSlices5", "sameStringSlices6", "sameStringSlices7", "sameStringSlices8",
		} {
			assert.False(t, objDiff.IsChangedProp(prop))
		}
	})

	t.Run("string slices are different", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("differentStringSlices1", []string{"aa", "bb", "cc"}, []string{"aa", "bb", "cc", "dd"}).
			WithProp("differentStringSlices2", []string{"aa", "bb", "cc"}, []interface{}{"aa", "bb", "cc", "dd"}).
			WithProp("differentStringSlices3", []interface{}{"aa", "bb", "cc"}, []string{"aa", "bb", "cc", "dd"}).
			WithProp("differentStringSlices4", []interface{}{"aa", "bb", "cc"}, []interface{}{"aa", "bb", "cc", "dd"}).
			WithProp("differentStringSlices5", []string{"aa", "bb", "cc"}, []string{"cc", "bb", "aa"}).
			WithProp("differentStringSlices6", []string{"aa", "bb", "cc"}, []interface{}{"cc", "bb", "aa"}).
			WithProp("differentStringSlices7", []interface{}{"aa", "bb", "cc"}, []string{"cc", "bb", "aa"}).
			WithProp("differentStringSlices8", []interface{}{"aa", "bb", "cc"}, []interface{}{"cc", "bb", "aa"})

		for _, prop := range []string{
			"differentStringSlices1", "differentStringSlices2", "differentStringSlices3", "differentStringSlices4",
			"differentStringSlices5", "differentStringSlices6", "differentStringSlices7", "differentStringSlices8",
		} {
			assert.True(t, objDiff.IsChangedProp(prop))
		}
	})

	t.Run("nils are different", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("nil1", "some value", nil).
			WithProp("nil2", nil, "some value").
			WithProp("nil3", []string{"some value"}, nil).
			WithProp("nil4", nil, []string{"some value"})

		for _, prop := range []string{"nil1", "nil2", "nil3", "nil4"} {
			assert.True(t, objDiff.IsChangedProp(prop))
		}
	})

	t.Run("not set is the same", func(t *testing.T) {
		objDiff := NewObjectDiff(nil)

		assert.False(t, objDiff.IsChangedProp("notSet"))
	})

	t.Run("non strings are different", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("float1", 1.23, 1.23).
			WithProp("float2", 1.23, 1.234).
			WithProp("int1", 1, 1).
			WithProp("int2", 1, 2).
			WithProp("bool1", true, true).
			WithProp("bool2", true, false).
			WithProp("floatSlice1", []float64{1.23}, []float64{1.23}).
			WithProp("floatSlice2", []float64{1.23}, []float64{1.23, 1.234}).
			WithProp("intSlice1", []int64{1}, []int64{1}).
			WithProp("intSlice2", []int64{1}, []int64{1, 2}).
			WithProp("boolSlice1", []bool{false}, []bool{false}).
			WithProp("boolSlice2", []bool{false}, []bool{false, true})

		for _, prop := range []string{
			"float1", "float2", "int1", "int2", "bool1", "bool2",
			"floatSlice1", "floatSlice2", "intSlice1", "intSlice2", "boolSlice1", "boolSlice2",
		} {
			assert.True(t, objDiff.IsChangedProp(prop))
		}
	})
}
