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

package search

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// testSchema builds an in-memory schema: an Article collection with one
// property of each filterable type plus a cross-reference to Category.
func testSchema() schema.Schema {
	return schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
		{
			Class: "Article",
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "year", DataType: []string{"int"}},
				{Name: "rating", DataType: []string{"number"}},
				{Name: "published", DataType: []string{"boolean"}},
				{Name: "publishDate", DataType: []string{"date"}},
				{Name: "location", DataType: []string{"geoCoordinates"}},
				{Name: "tags", DataType: []string{"text[]"}},
				{Name: "inCategory", DataType: []string{"Category"}},
			},
		},
		{
			Class: "Category",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		},
	}}}
}

func TestToWhereFilter_ScalarTypes(t *testing.T) {
	sch := testSchema()
	tests := []struct {
		name   string
		filter Filter
		verify func(t *testing.T, wf *models.WhereFilter)
	}{
		{
			name:   "text Equal",
			filter: Filter{Operator: "Equal", Path: []string{"title"}, Value: "Go"},
			verify: func(t *testing.T, wf *models.WhereFilter) {
				require.NotNil(t, wf.ValueText)
				require.Equal(t, "Go", *wf.ValueText)
			},
		},
		{
			name:   "int GreaterThanEqual",
			filter: Filter{Operator: "GreaterThanEqual", Path: []string{"year"}, Value: 2020},
			verify: func(t *testing.T, wf *models.WhereFilter) {
				require.NotNil(t, wf.ValueInt)
				require.Equal(t, int64(2020), *wf.ValueInt)
			},
		},
		{
			name:   "number Equal",
			filter: Filter{Operator: "Equal", Path: []string{"rating"}, Value: 4.5},
			verify: func(t *testing.T, wf *models.WhereFilter) {
				require.NotNil(t, wf.ValueNumber)
				require.Equal(t, 4.5, *wf.ValueNumber)
			},
		},
		{
			name:   "boolean Equal",
			filter: Filter{Operator: "Equal", Path: []string{"published"}, Value: true},
			verify: func(t *testing.T, wf *models.WhereFilter) {
				require.NotNil(t, wf.ValueBoolean)
				require.True(t, *wf.ValueBoolean)
			},
		},
		{
			name:   "date GreaterThan",
			filter: Filter{Operator: "GreaterThan", Path: []string{"publishDate"}, Value: "2021-01-01T00:00:00Z"},
			verify: func(t *testing.T, wf *models.WhereFilter) {
				require.NotNil(t, wf.ValueDate)
				require.Equal(t, "2021-01-01T00:00:00Z", *wf.ValueDate)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf, err := tt.filter.ToWhereFilter(sch, "Article")
			require.NoError(t, err)
			require.Equal(t, tt.filter.Operator, wf.Operator)
			tt.verify(t, wf)
		})
	}
}

func TestToWhereFilter_IsNull(t *testing.T) {
	sch := testSchema()
	wf, err := Filter{Operator: "IsNull", Path: []string{"title"}, Value: true}.ToWhereFilter(sch, "Article")
	require.NoError(t, err)
	require.NotNil(t, wf.ValueBoolean)
	require.True(t, *wf.ValueBoolean)

	_, err = Filter{Operator: "IsNull", Path: []string{"title"}, Value: "yes"}.ToWhereFilter(sch, "Article")
	require.ErrorContains(t, err, "requires a boolean value")
}

func TestToWhereFilter_ContainsAny(t *testing.T) {
	sch := testSchema()
	wf, err := Filter{
		Operator: "ContainsAny",
		Path:     []string{"tags"},
		Value:    []any{"go", "rust"},
	}.ToWhereFilter(sch, "Article")
	require.NoError(t, err)
	require.Equal(t, []string{"go", "rust"}, wf.ValueTextArray)

	_, err = Filter{Operator: "ContainsAny", Path: []string{"tags"}, Value: "go"}.ToWhereFilter(sch, "Article")
	require.ErrorContains(t, err, "requires an array value")
}

func TestToWhereFilter_WithinGeoRange(t *testing.T) {
	sch := testSchema()
	wf, err := Filter{
		Operator: "WithinGeoRange",
		Path:     []string{"location"},
		Value:    map[string]any{"latitude": 52.0, "longitude": 4.3, "distance": 1000.0},
	}.ToWhereFilter(sch, "Article")
	require.NoError(t, err)
	require.NotNil(t, wf.ValueGeoRange)
	require.NotNil(t, wf.ValueGeoRange.GeoCoordinates)
	require.InDelta(t, 52.0, *wf.ValueGeoRange.GeoCoordinates.Latitude, 0.001)
	require.Equal(t, 1000.0, wf.ValueGeoRange.Distance.Max)

	_, err = Filter{Operator: "WithinGeoRange", Path: []string{"title"}, Value: map[string]any{}}.ToWhereFilter(sch, "Article")
	require.ErrorContains(t, err, "requires a geoCoordinates property")
}

func TestToWhereFilter_Logical(t *testing.T) {
	sch := testSchema()
	wf, err := Filter{
		Operator: "And",
		Operands: []Filter{
			{Operator: "Equal", Path: []string{"title"}, Value: "Go"},
			{Operator: "GreaterThanEqual", Path: []string{"year"}, Value: 2020},
		},
	}.ToWhereFilter(sch, "Article")
	require.NoError(t, err)
	require.Equal(t, "And", wf.Operator)
	require.Len(t, wf.Operands, 2)
	require.Equal(t, "Go", *wf.Operands[0].ValueText)
	require.Equal(t, int64(2020), *wf.Operands[1].ValueInt)

	_, err = Filter{Operator: "And"}.ToWhereFilter(sch, "Article")
	require.ErrorContains(t, err, "requires at least one entry in `operands`")
}

func TestToWhereFilter_CrossReferencePath(t *testing.T) {
	sch := testSchema()
	wf, err := Filter{
		Operator: "Equal",
		Path:     []string{"inCategory", "Category", "name"},
		Value:    "Tech",
	}.ToWhereFilter(sch, "Article")
	require.NoError(t, err)
	require.NotNil(t, wf.ValueText)
	require.Equal(t, "Tech", *wf.ValueText)

	_, err = Filter{
		Operator: "Equal",
		Path:     []string{"inCategory", "Nonexistent", "name"},
		Value:    "Tech",
	}.ToWhereFilter(sch, "Article")
	require.ErrorContains(t, err, "does not point to collection")
}

func TestToWhereFilter_Errors(t *testing.T) {
	sch := testSchema()
	tests := []struct {
		name    string
		filter  Filter
		wantErr string
	}{
		{
			name:    "unknown operator",
			filter:  Filter{Operator: "Greaterish", Path: []string{"year"}, Value: 1},
			wantErr: "unknown filter operator",
		},
		{
			name:    "unknown property lists available",
			filter:  Filter{Operator: "Equal", Path: []string{"titel"}, Value: "x"},
			wantErr: "available properties",
		},
		{
			name:    "type mismatch",
			filter:  Filter{Operator: "Equal", Path: []string{"year"}, Value: "twenty"},
			wantErr: "expected an integer",
		},
		{
			name:    "Like on non-text",
			filter:  Filter{Operator: "Like", Path: []string{"year"}, Value: "20*"},
			wantErr: "operator Like requires a text property",
		},
		{
			name:    "value operator without path",
			filter:  Filter{Operator: "Equal", Value: "x"},
			wantErr: "requires a `path`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.filter.ToWhereFilter(sch, "Article")
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
