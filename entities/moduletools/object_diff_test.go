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
			WithProp("sameStringSlices2", []string{}, []string{})

		assert.False(t, objDiff.IsChangedProp("sameStringSlices1"))
		assert.False(t, objDiff.IsChangedProp("sameStringSlices2"))
	})

	t.Run("string slices are different", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("differentStringSlices1", []string{"aa", "bb", "cc"}, []string{"aa", "bb", "cc", "dd"}).
			WithProp("differentStringSlices2", []string{"aa", "bb", "cc"}, []string{"cc", "bb", "aa"})

		assert.True(t, objDiff.IsChangedProp("differentStringSlices1"))
		assert.True(t, objDiff.IsChangedProp("differentStringSlices2"))
	})

	t.Run("nils are different", func(t *testing.T) {
		objDiff := NewObjectDiff(nil).
			WithProp("nil1", "some value", nil).
			WithProp("nil2", nil, "some value").
			WithProp("nil3", []string{"some value"}, nil).
			WithProp("nil4", nil, []string{"some value"})

		assert.True(t, objDiff.IsChangedProp("nil1"))
		assert.True(t, objDiff.IsChangedProp("nil2"))
		assert.True(t, objDiff.IsChangedProp("nil3"))
		assert.True(t, objDiff.IsChangedProp("nil4"))
	})

	t.Run("not set is the same", func(t *testing.T) {
		objDiff := NewObjectDiff(nil)

		assert.False(t, objDiff.IsChangedProp("notSet"))
	})

	t.Run("non string types are different", func(t *testing.T) {
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

		assert.True(t, objDiff.IsChangedProp("float1"))
		assert.True(t, objDiff.IsChangedProp("float2"))
		assert.True(t, objDiff.IsChangedProp("int1"))
		assert.True(t, objDiff.IsChangedProp("int2"))
		assert.True(t, objDiff.IsChangedProp("bool1"))
		assert.True(t, objDiff.IsChangedProp("bool2"))
		assert.True(t, objDiff.IsChangedProp("floatSlice1"))
		assert.True(t, objDiff.IsChangedProp("floatSlice2"))
		assert.True(t, objDiff.IsChangedProp("intSlice1"))
		assert.True(t, objDiff.IsChangedProp("intSlice2"))
		assert.True(t, objDiff.IsChangedProp("boolSlice1"))
		assert.True(t, objDiff.IsChangedProp("boolSlice2"))
	})
}
