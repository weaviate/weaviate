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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestIsArrayDataType(t *testing.T) {
	type args struct {
		dt []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "is string array",
			args: args{
				dt: DataTypeTextArray.PropString(),
			},
			want: true,
		},
		{
			name: "is not string array",
			args: args{
				dt: DataTypeText.PropString(),
			},
			want: false,
		},
		{
			name: "is text array",
			args: args{
				dt: []string{"text[]"},
			},
			want: true,
		},
		{
			name: "is not text array",
			args: args{
				dt: []string{"text"},
			},
			want: false,
		},
		{
			name: "is number array",
			args: args{
				dt: []string{"number[]"},
			},
			want: true,
		},
		{
			name: "is not number array",
			args: args{
				dt: []string{"number"},
			},
			want: false,
		},
		{
			name: "is int array",
			args: args{
				dt: []string{"int[]"},
			},
			want: true,
		},
		{
			name: "is not int array",
			args: args{
				dt: []string{"int"},
			},
			want: false,
		},
		{
			name: "is not uuid array",
			args: args{
				dt: []string{"uuid"},
			},
			want: false,
		},
		{
			name: "is uuid array",
			args: args{
				dt: []string{"uuid[]"},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsArrayDataType(tt.args.dt); got != tt.want {
				t.Errorf("IsArrayDataType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsRefDataType_QualifiedNames(t *testing.T) {
	cases := []struct {
		name string
		dt   []string
		want bool
	}{
		{name: "bare ref", dt: []string{"Movies"}, want: true},
		{name: "qualified own-ns ref", dt: []string{"customer1:Movies"}, want: true},
		{name: "qualified multi-target ref", dt: []string{"customer1:Movies", "customer1:Books"}, want: true},
		{name: "bare primitive text", dt: []string{"text"}, want: false},
		{name: "bare primitive int", dt: []string{"int"}, want: false},
		{name: "bare primitive text[]", dt: []string{"text[]"}, want: false},
		{name: "bare nested object", dt: []string{"object"}, want: false},
		{name: "bare nested object[]", dt: []string{"object[]"}, want: false},
		{name: "empty slice", dt: []string{}, want: false},
		{name: "empty string element", dt: []string{""}, want: false},
		{name: "only namespace separator", dt: []string{":"}, want: false},
		{name: "trailing colon", dt: []string{"customer1:"}, want: false},
		// Cannot arise in practice (class names must start uppercase per
		// ClassNameRegexCore), but the rule is pinned: lowercase class
		// portion is NOT classified as ref.
		{name: "lowercase class portion", dt: []string{"customer1:text"}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsRefDataType(tc.dt))
		})
	}
}

func TestGetPropertyDataType_QualifiedRef(t *testing.T) {
	class := &models.Class{
		Class: "customer1:Library",
		Properties: []*models.Property{
			{Name: "watched", DataType: []string{"customer1:Movies"}},
			{Name: "title", DataType: []string{"text"}},
		},
	}
	gotRef, err := GetPropertyDataType(class, "watched")
	require.NoError(t, err)
	require.NotNil(t, gotRef)
	assert.Equal(t, DataTypeCRef, *gotRef)

	gotPrim, err := GetPropertyDataType(class, "title")
	require.NoError(t, err)
	require.NotNil(t, gotPrim)
	assert.Equal(t, DataTypeText, *gotPrim)
}

type fakeNestedProperty struct {
	name   string
	nested []*models.NestedProperty
}

func (f fakeNestedProperty) GetName() string                               { return f.name }
func (f fakeNestedProperty) GetNestedProperties() []*models.NestedProperty { return f.nested }

func TestGetNestedPropertyDataType_QualifiedRef(t *testing.T) {
	// NestedProperty.DataType cross-refs are rejected upstream at
	// validation.go:95, so this shape cannot reach production. The test
	// pins the helper's behaviour symmetric with GetPropertyDataType so a
	// future refactor cannot regress the dedup.
	p := fakeNestedProperty{
		name: "outer",
		nested: []*models.NestedProperty{
			{Name: "ref", DataType: []string{"customer1:Movies"}},
			{Name: "label", DataType: []string{"text"}},
		},
	}
	gotRef, err := GetNestedPropertyDataType(p, "ref")
	require.NoError(t, err)
	require.NotNil(t, gotRef)
	assert.Equal(t, DataTypeCRef, *gotRef)

	gotPrim, err := GetNestedPropertyDataType(p, "label")
	require.NoError(t, err)
	require.NotNil(t, gotPrim)
	assert.Equal(t, DataTypeText, *gotPrim)
}
