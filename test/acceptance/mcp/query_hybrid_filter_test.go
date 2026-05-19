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

package mcp

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// This file exercises the weaviate-query-hybrid `filters` DSL end-to-end,
// covering every operator and value type against a fixture rich enough to
// produce deterministic result counts. The fixture has a property of each
// filterable type plus a cross-reference to a second collection.

const (
	filterArticleClass  = "TestFilterArticle"
	filterCategoryClass = "TestFilterCategory"
)

var (
	catTechID    = strfmt.UUID("a1111111-0000-0000-0000-000000000001")
	catScienceID = strfmt.UUID("a1111111-0000-0000-0000-000000000002")
)

// setupQueryHybridFilterTest creates the category and article collections,
// seeds six articles (each with "research" in contents so a pure-BM25 query
// for "research" returns all of them before filtering), and returns the
// article class, context, cleanup, and the pure-BM25 alpha.
func setupQueryHybridFilterTest(t *testing.T) (*models.Class, context.Context, func(), float64) {
	t.Helper()
	helper.SetupClient(testServerAddr)

	category := &models.Class{
		Class: filterCategoryClass,
		Properties: []*models.Property{
			{Name: "name", DataType: []string{"text"}},
		},
	}
	article := &models.Class{
		Class: filterArticleClass,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "contents", DataType: []string{"text"}},
			{Name: "year", DataType: []string{"int"}},
			{Name: "rating", DataType: []string{"number"}},
			{Name: "featured", DataType: []string{"boolean"}},
			{Name: "publishDate", DataType: []string{"date"}},
			{Name: "tags", DataType: []string{"text[]"}},
			{Name: "location", DataType: []string{"geoCoordinates"}},
			{Name: "inCategory", DataType: []string{filterCategoryClass}},
		},
	}

	// The category class must exist before the article class can reference it.
	helper.DeleteClassAuth(t, filterArticleClass, testAPIKey)
	helper.DeleteClassAuth(t, filterCategoryClass, testAPIKey)
	helper.CreateClassAuth(t, category, testAPIKey)
	helper.CreateClassAuth(t, article, testAPIKey)

	insertFilterTestData(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cleanup := func() {
		cancel()
		helper.DeleteClassAuth(t, filterArticleClass, testAPIKey)
		helper.DeleteClassAuth(t, filterCategoryClass, testAPIKey)
	}
	return article, ctx, cleanup, 0.0
}

func insertFilterTestData(t *testing.T) {
	t.Helper()

	helper.CreateObjectsBatchAuth(t, []*models.Object{
		{Class: filterCategoryClass, ID: catTechID, Properties: map[string]interface{}{"name": "Technology"}},
		{Class: filterCategoryClass, ID: catScienceID, Properties: map[string]interface{}{"name": "Science"}},
	}, testAPIKey)

	catRef := func(id strfmt.UUID) []interface{} {
		return []interface{}{
			map[string]interface{}{"beacon": string(helper.NewBeacon(filterCategoryClass, id))},
		}
	}
	geo := func(lat, lon float64) map[string]interface{} {
		return map[string]interface{}{"latitude": lat, "longitude": lon}
	}

	helper.CreateObjectsBatchAuth(t, []*models.Object{
		{Class: filterArticleClass, Properties: map[string]interface{}{
			"title": "Introduction to Go", "contents": "A research piece on Go",
			"year": 2019, "rating": 4.5, "featured": true,
			"publishDate": "2019-01-01T00:00:00Z",
			"tags":        []interface{}{"go", "backend"},
			"location":    geo(52.3676, 4.9041), // Amsterdam
			"inCategory":  catRef(catTechID),
		}},
		{Class: filterArticleClass, Properties: map[string]interface{}{
			"title": "Advanced Rust Systems", "contents": "Deep research into Rust",
			"year": 2021, "rating": 4.8, "featured": true,
			"publishDate": "2021-06-01T00:00:00Z",
			"tags":        []interface{}{"rust", "systems"},
			"location":    geo(52.5200, 13.4050), // Berlin
			"inCategory":  catRef(catTechID),
		}},
		{Class: filterArticleClass, Properties: map[string]interface{}{
			"title": "Python Fundamentals", "contents": "Beginner research with Python",
			"year": 2020, "rating": 3.9, "featured": false,
			"publishDate": "2020-03-01T00:00:00Z",
			"tags":        []interface{}{"python"},
			"location":    geo(48.8566, 2.3522), // Paris
			"inCategory":  catRef(catTechID),
		}},
		{Class: filterArticleClass, Properties: map[string]interface{}{
			"title": "Quantum Computing Theory", "contents": "Cutting-edge research on quantum",
			"year": 2022, "rating": 4.2, "featured": false,
			"publishDate": "2022-09-01T00:00:00Z",
			"tags":        []interface{}{"quantum", "physics"},
			"location":    geo(51.5074, -0.1278), // London
			"inCategory":  catRef(catScienceID),
		}},
		{Class: filterArticleClass, Properties: map[string]interface{}{
			"title": "Climate Modeling", "contents": "Applied research on climate",
			"year": 2023, "rating": 4.0, "featured": true,
			"publishDate": "2023-02-01T00:00:00Z",
			"tags":        []interface{}{"climate", "physics"},
			"location":    geo(51.5074, -0.1278), // London
			"inCategory":  catRef(catScienceID),
		}},
		{Class: filterArticleClass, Properties: map[string]interface{}{
			"title": "Historical Archive", "contents": "Old research notes",
			"year": 2015, "rating": 2.5, "featured": false,
			// publishDate intentionally omitted — exercises the IsNull operator.
			"tags":       []interface{}{"archive"},
			"location":   geo(35.6762, 139.6503), // Tokyo
			"inCategory": catRef(catScienceID),
		}},
	}, testAPIKey)
}

// TestQueryHybridFilters exercises every filter operator end-to-end. The query
// "research" matches all six articles under pure BM25, so the asserted count
// reflects exactly what the filter selects.
func TestQueryHybridFilters(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridFilterTest(t)
	defer cleanup()

	tests := []struct {
		name      string
		filter    *search.Filter
		wantCount int
	}{
		{
			name:      "Equal on text",
			filter:    &search.Filter{Operator: "Equal", Path: []string{"title"}, Value: "Python Fundamentals"},
			wantCount: 1,
		},
		{
			name:      "Equal on int",
			filter:    &search.Filter{Operator: "Equal", Path: []string{"year"}, Value: 2021},
			wantCount: 1,
		},
		{
			name:      "Equal on boolean",
			filter:    &search.Filter{Operator: "Equal", Path: []string{"featured"}, Value: true},
			wantCount: 3,
		},
		{
			name:      "Equal on number",
			filter:    &search.Filter{Operator: "Equal", Path: []string{"rating"}, Value: 4.8},
			wantCount: 1,
		},
		{
			name:      "NotEqual on int",
			filter:    &search.Filter{Operator: "NotEqual", Path: []string{"year"}, Value: 2020},
			wantCount: 5,
		},
		{
			name:      "GreaterThan on int",
			filter:    &search.Filter{Operator: "GreaterThan", Path: []string{"year"}, Value: 2021},
			wantCount: 2,
		},
		{
			name:      "GreaterThanEqual on int",
			filter:    &search.Filter{Operator: "GreaterThanEqual", Path: []string{"year"}, Value: 2021},
			wantCount: 3,
		},
		{
			name:      "LessThan on int",
			filter:    &search.Filter{Operator: "LessThan", Path: []string{"year"}, Value: 2020},
			wantCount: 2,
		},
		{
			name:      "LessThanEqual on int",
			filter:    &search.Filter{Operator: "LessThanEqual", Path: []string{"year"}, Value: 2020},
			wantCount: 3,
		},
		{
			name:      "GreaterThanEqual on number",
			filter:    &search.Filter{Operator: "GreaterThanEqual", Path: []string{"rating"}, Value: 4.5},
			wantCount: 2,
		},
		{
			name:      "Like with leading wildcard",
			filter:    &search.Filter{Operator: "Like", Path: []string{"title"}, Value: "*Systems"},
			wantCount: 1,
		},
		{
			name:      "GreaterThan on date",
			filter:    &search.Filter{Operator: "GreaterThan", Path: []string{"publishDate"}, Value: "2021-12-31T23:59:59Z"},
			wantCount: 2,
		},
		{
			name:      "LessThanEqual on date",
			filter:    &search.Filter{Operator: "LessThanEqual", Path: []string{"publishDate"}, Value: "2020-12-31T23:59:59Z"},
			wantCount: 2,
		},
		{
			name: "And of two conditions",
			filter: &search.Filter{Operator: "And", Operands: []search.Filter{
				{Operator: "Equal", Path: []string{"featured"}, Value: true},
				{Operator: "GreaterThanEqual", Path: []string{"year"}, Value: 2021},
			}},
			wantCount: 2,
		},
		{
			name: "Or of two conditions",
			filter: &search.Filter{Operator: "Or", Operands: []search.Filter{
				{Operator: "Equal", Path: []string{"year"}, Value: 2019},
				{Operator: "Equal", Path: []string{"year"}, Value: 2015},
			}},
			wantCount: 2,
		},
		{
			name: "Nested And containing an Or",
			filter: &search.Filter{Operator: "And", Operands: []search.Filter{
				{Operator: "Equal", Path: []string{"featured"}, Value: true},
				{Operator: "Or", Operands: []search.Filter{
					{Operator: "Equal", Path: []string{"year"}, Value: 2019},
					{Operator: "Equal", Path: []string{"year"}, Value: 2023},
				}},
			}},
			wantCount: 2,
		},
		{
			name: "Not negates a condition",
			filter: &search.Filter{Operator: "Not", Operands: []search.Filter{
				{Operator: "Equal", Path: []string{"featured"}, Value: true},
			}},
			wantCount: 3,
		},
		{
			name:      "ContainsAny on text array",
			filter:    &search.Filter{Operator: "ContainsAny", Path: []string{"tags"}, Value: []any{"go", "python"}},
			wantCount: 2,
		},
		{
			name:      "ContainsAll on text array",
			filter:    &search.Filter{Operator: "ContainsAll", Path: []string{"tags"}, Value: []any{"quantum", "physics"}},
			wantCount: 1,
		},
		{
			name:      "ContainsNone on text array",
			filter:    &search.Filter{Operator: "ContainsNone", Path: []string{"tags"}, Value: []any{"go", "rust", "python"}},
			wantCount: 3,
		},
		{
			name:      "IsNull on a missing property",
			filter:    &search.Filter{Operator: "IsNull", Path: []string{"publishDate"}, Value: true},
			wantCount: 1,
		},
		{
			name: "WithinGeoRange",
			filter: &search.Filter{
				Operator: "WithinGeoRange",
				Path:     []string{"location"},
				Value:    map[string]any{"latitude": 51.5074, "longitude": -0.1278, "distance": 50000.0},
			},
			wantCount: 2,
		},
		{
			name:      "Equal across a cross-reference path",
			filter:    &search.Filter{Operator: "Equal", Path: []string{"inCategory", filterCategoryClass, "name"}, Value: "Science"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executeHybridQueryWithResults(t, ctx, &search.QueryHybridArgs{
				CollectionName: cls.Class,
				Query:          "research",
				Alpha:          &alpha,
				Filters:        tt.filter,
			}, tt.wantCount)
		})
	}
}

// TestQueryHybridFilterInvalid verifies that malformed filters fail with an
// actionable error rather than silently matching nothing.
func TestQueryHybridFilterInvalid(t *testing.T) {
	cls, ctx, cleanup, alpha := setupQueryHybridFilterTest(t)
	defer cleanup()

	tests := []struct {
		name   string
		filter *search.Filter
	}{
		{
			name:   "unknown property",
			filter: &search.Filter{Operator: "Equal", Path: []string{"nonexistent"}, Value: "x"},
		},
		{
			name:   "unknown operator",
			filter: &search.Filter{Operator: "Approximately", Path: []string{"year"}, Value: 2020},
		},
		{
			name:   "type mismatch",
			filter: &search.Filter{Operator: "Equal", Path: []string{"year"}, Value: "not-a-number"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var results *search.QueryHybridResp
			err := helper.CallToolOnce(ctx, t, toolNameQueryHybrid, &search.QueryHybridArgs{
				CollectionName: cls.Class,
				Query:          "research",
				Alpha:          &alpha,
				Filters:        tt.filter,
			}, &results, testAPIKey)
			require.Error(t, err, "an invalid filter must surface an error")
		})
	}
}
