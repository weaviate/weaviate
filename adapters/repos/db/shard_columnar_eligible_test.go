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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestColumnarContainsEligible pins which properties may carry the columnar
// accelerator. The accelerator applies a flush's deletions across the whole
// result, so it is only sound where a document owns exactly one key: anything
// that spreads a document over several keys must be refused, or losing one of
// that document's values would drop it from all of them.
func TestColumnarContainsEligible(t *testing.T) {
	t.Setenv(entcfg.EnvEnableColumnarContains, "true")
	s := &Shard{}

	prop := func(dataType string, tokenization string) *models.Property {
		return &models.Property{
			Name:         "prop",
			DataType:     []string{dataType},
			Tokenization: tokenization,
		}
	}

	tests := []struct {
		name string
		prop *models.Property
		want bool
	}{
		{
			name: "text with field tokenization is one key per doc",
			prop: prop(string(schema.DataTypeText), models.PropertyTokenizationField),
			want: true,
		},
		{
			name: "text with word tokenization spreads a doc over its terms",
			prop: prop(string(schema.DataTypeText), models.PropertyTokenizationWord),
			want: false,
		},
		{
			name: "text with whitespace tokenization spreads a doc over its terms",
			prop: prop(string(schema.DataTypeText), models.PropertyTokenizationWhitespace),
			want: false,
		},
		{
			name: "text array holds one key per element",
			prop: prop(string(schema.DataTypeTextArray), models.PropertyTokenizationField),
			want: false,
		},
		{name: "int", prop: prop(string(schema.DataTypeInt), ""), want: true},
		{name: "number", prop: prop(string(schema.DataTypeNumber), ""), want: true},
		{name: "boolean", prop: prop(string(schema.DataTypeBoolean), ""), want: true},
		{name: "date", prop: prop(string(schema.DataTypeDate), ""), want: true},
		{name: "int array", prop: prop(string(schema.DataTypeIntArray), ""), want: false},
		{name: "number array", prop: prop(string(schema.DataTypeNumberArray), ""), want: false},
		{name: "boolean array", prop: prop(string(schema.DataTypeBooleanArray), ""), want: false},
		{name: "date array", prop: prop(string(schema.DataTypeDateArray), ""), want: false},
		{
			name: "geo coordinates are not a plain key column",
			prop: prop(string(schema.DataTypeGeoCoordinates), ""),
			want: false,
		},
		{name: "blob", prop: prop(string(schema.DataTypeBlob), ""), want: false},
		{
			name: "reference holds one key per beacon",
			prop: prop("SomeOtherClass", ""),
			want: false,
		},
		{
			name: "cross-reference to several classes",
			prop: &models.Property{Name: "prop", DataType: []string{"ClassA", "ClassB"}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, s.columnarContainsEligible(tt.prop))
		})
	}
}

// TestColumnarContainsEligibleUsesTokenizationOverlay pins that eligibility is
// judged by the same resolver the query path uses, so a property whose
// tokenization is being migrated is read as the tokenization its bucket
// actually holds rather than the schema value the migration has not reached yet.
func TestColumnarContainsEligibleUsesTokenizationOverlay(t *testing.T) {
	t.Setenv(entcfg.EnvEnableColumnarContains, "true")
	s := &Shard{}
	fieldProp := &models.Property{
		Name:         "prop",
		DataType:     []string{string(schema.DataTypeText)},
		Tokenization: models.PropertyTokenizationField,
	}
	require.True(t, s.columnarContainsEligible(fieldProp))

	// migrating field -> word: the overlay reports the live tokenization before
	// the schema catches up, and the property stops qualifying immediately
	s.SetTokenizationOverlay("prop", models.PropertyTokenizationWord)
	require.False(t, s.columnarContainsEligible(fieldProp),
		"a property being retokenized away from field must stop qualifying")
}

// TestColumnarContainsEligibleGateOff pins that nothing attaches while the
// feature flag is off, whatever the property looks like.
func TestColumnarContainsEligibleGateOff(t *testing.T) {
	t.Setenv(entcfg.EnvEnableColumnarContains, "")
	s := &Shard{}
	require.False(t, s.columnarContainsEligible(&models.Property{
		Name:         "prop",
		DataType:     []string{string(schema.DataTypeText)},
		Tokenization: models.PropertyTokenizationField,
	}))
}
