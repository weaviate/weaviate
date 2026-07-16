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

package deepcopy

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// BucketGeneration is the RAFT-replicated reindex-generation counter
// (weaviate/weaviate#11689); a deep copy must preserve it or migrations
// relying on the copy resolves the wrong LSM bucket.
func TestProp_PreservesBucketGeneration(t *testing.T) {
	original := &models.Property{
		Name:             "prop",
		BucketGeneration: 7,
	}

	cp := Prop(original)

	assert.Equal(t, int64(7), cp.BucketGeneration)

	// mutating the original must not affect the copy
	original.BucketGeneration = 99
	assert.Equal(t, int64(7), cp.BucketGeneration)
}

func TestClass_PreservesPropertyBucketGeneration(t *testing.T) {
	original := &models.Class{
		Class: "Test",
		Properties: []*models.Property{
			{Name: "prop", BucketGeneration: 3},
		},
	}

	cp := Class(original)

	assert.Equal(t, int64(3), cp.Properties[0].BucketGeneration)
}

// populate walks v (addressable) by reflect.Kind and sets every field it
// finds to a representative non-zero value, recursing into pointers,
// structs, and slice elements. It is driven entirely by shape, not by field
// name, so a field added to models.Property/NestedProperty in the future
// gets populated automatically without touching this file — that's what
// lets TestProp_CopiesAllExportedFields (and the NestedProperty analogue)
// go red on its own the moment Prop()/NestedProp() falls behind the struct
// again, instead of relying on someone remembering to extend the test.
//
// depth bounds recursion because NestedProperty is self-referential
// (NestedProperties []*NestedProperty); it terminates the walk a couple of
// levels in rather than looping forever.
func populate(v reflect.Value, depth int) {
	if depth <= 0 || !v.CanSet() {
		return
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		populate(v.Elem(), depth-1)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			populate(v.Field(i), depth)
		}
	case reflect.Slice:
		if v.Len() == 0 {
			elem := reflect.New(v.Type().Elem()).Elem()
			populate(elem, depth-1)
			v.Set(reflect.Append(v, elem))
		}
	case reflect.Map:
		if v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
	case reflect.Interface:
		if v.IsNil() {
			v.Set(reflect.ValueOf(map[string]interface{}{"k": "v"}))
		}
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	default:
		// non-settable or exotic kinds (chan, func, complex, ...) don't occur
		// in the models structs this walker targets; leave them zero
	}
}

// zeroExportedFields reports the names of v's (a struct) exported fields
// that are still at their zero value.
func zeroExportedFields(v reflect.Value) []string {
	var zero []string
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		if t.Field(i).PkgPath != "" { // unexported
			continue
		}
		if v.Field(i).IsZero() {
			zero = append(zero, t.Field(i).Name)
		}
	}
	return zero
}

// TestProp_CopiesAllExportedFields is a coverage guard, not a value check:
// it fills every exported field of a models.Property (recursively, incl.
// NestedProperties) via reflection, copies it through Prop(), and fails if
// any field on the copy is still zero. Because populate() walks by Kind
// rather than by field name, a field added to models.Property later is
// covered automatically — the guard goes red on its own if Prop() isn't
// updated to copy it (this is the exhaustiveness gap left by #11689).
func TestProp_CopiesAllExportedFields(t *testing.T) {
	original := &models.Property{}
	populate(reflect.ValueOf(original).Elem(), 6)
	require.NotEmpty(t, original.NestedProperties, "test setup: populate() should have filled NestedProperties")
	require.NotEmpty(t, original.NestedProperties[0].NestedProperties, "test setup: populate() should have nested two levels deep")

	cp := Prop(original)

	if zero := zeroExportedFields(reflect.ValueOf(*cp)); len(zero) > 0 {
		t.Fatalf("Prop() left these models.Property fields at their zero value (not copied): %v", zero)
	}

	// Targeted deep-copy checks: mutating the source after the copy must
	// not be observable on the copy, proving these aren't shared refs.
	*original.IndexFilterable = !*original.IndexFilterable
	assert.NotEqual(t, *original.IndexFilterable, *cp.IndexFilterable, "IndexFilterable must be a distinct pointer")

	original.NestedProperties[0].Name = "mutated-level-1"
	assert.NotEqual(t, "mutated-level-1", cp.NestedProperties[0].Name, "NestedProperties must be deep-copied, not a shared slice/pointer")

	original.NestedProperties[0].NestedProperties[0].Name = "mutated-level-2"
	assert.NotEqual(t, "mutated-level-2", cp.NestedProperties[0].NestedProperties[0].Name, "NestedProperties must be deep-copied recursively, not just one level")

	original.TextAnalyzer.StopwordPreset = "mutated"
	assert.NotEqual(t, "mutated", cp.TextAnalyzer.StopwordPreset, "TextAnalyzer must be a distinct pointer")
}

// TestNestedProp_CopiesAllExportedFields is the NestedProperty analogue of
// TestProp_CopiesAllExportedFields above, exercised directly since
// NestedProp() is also hand-written and can independently fall behind
// models.NestedProperty.
func TestNestedProp_CopiesAllExportedFields(t *testing.T) {
	original := &models.NestedProperty{}
	populate(reflect.ValueOf(original).Elem(), 6)
	require.NotEmpty(t, original.NestedProperties, "test setup: populate() should have filled NestedProperties")

	cp := NestedProp(original)

	if zero := zeroExportedFields(reflect.ValueOf(*cp)); len(zero) > 0 {
		t.Fatalf("NestedProp() left these models.NestedProperty fields at their zero value (not copied): %v", zero)
	}

	*original.IndexFilterable = !*original.IndexFilterable
	assert.NotEqual(t, *original.IndexFilterable, *cp.IndexFilterable, "IndexFilterable must be a distinct pointer")

	original.NestedProperties[0].Name = "mutated"
	assert.NotEqual(t, "mutated", cp.NestedProperties[0].Name, "NestedProperties must be deep-copied, not a shared slice/pointer")

	original.TextAnalyzer.StopwordPreset = "mutated"
	assert.NotEqual(t, "mutated", cp.TextAnalyzer.StopwordPreset, "TextAnalyzer must be a distinct pointer")
}
