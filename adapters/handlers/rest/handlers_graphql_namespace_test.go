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

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	tailorincgraphql "github.com/tailor-platform/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

func TestPrefixGraphQLClassNames(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		ns       string
		expected string
	}{
		{
			name:     "simple Get query",
			query:    `{ Get { Articles { title } } }`,
			ns:       "tenanta",
			expected: `{ Get { Tenanta__Articles { title } } }`,
		},
		{
			name:     "Aggregate query",
			query:    `{ Aggregate { Articles { meta { count } } } }`,
			ns:       "tenanta",
			expected: `{ Aggregate { Tenanta__Articles { meta { count } } } }`,
		},
		{
			name:     "multiple classes in Get",
			query:    `{ Get { Articles { title } BlogPosts { content } } }`,
			ns:       "teambeta",
			expected: `{ Get { Teambeta__Articles { title } Teambeta__BlogPosts { content } } }`,
		},
		{
			name:     "already prefixed class name",
			query:    `{ Get { Tenanta__Articles { title } } }`,
			ns:       "tenanta",
			expected: `{ Get { Tenanta__Articles { title } } }`,
		},
		{
			name:     "query with arguments",
			query:    `{ Get { Articles(limit: 10) { title } } }`,
			ns:       "tenanta",
			expected: `{ Get { Tenanta__Articles(limit: 10) { title } } }`,
		},
		{
			name:     "query with newlines",
			query:    "{\n  Get {\n    Articles {\n      title\n    }\n  }\n}",
			ns:       "tenanta",
			expected: "{\n  Get {\n    Tenanta__Articles {\n      title\n    }\n  }\n}",
		},
		{
			name:     "both Get and Aggregate",
			query:    `{ Get { Articles { title } } Aggregate { BlogPosts { meta { count } } } }`,
			ns:       "myns",
			expected: `{ Get { Myns__Articles { title } } Aggregate { Myns__BlogPosts { meta { count } } } }`,
		},
		{
			name:     "empty query",
			query:    "",
			ns:       "tenanta",
			expected: "",
		},
		{
			name:     "query without class names",
			query:    `{ __schema { types { name } } }`,
			ns:       "tenanta",
			expected: `{ __schema { types { name } } }`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixGraphQLClassNames(tt.query, tt.ns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStripNamespacePrefixFromGraphQLResult(t *testing.T) {
	tests := []struct {
		name     string
		data     interface{}
		ns       string
		expected interface{}
	}{
		{
			name: "strip prefix from Get result",
			data: map[string]interface{}{
				"Get": map[string]interface{}{
					"Tenanta__Articles": []interface{}{
						map[string]interface{}{"title": "Hello"},
					},
				},
			},
			ns: "tenanta",
			expected: map[string]interface{}{
				"Get": map[string]interface{}{
					"Articles": []interface{}{
						map[string]interface{}{"title": "Hello"},
					},
				},
			},
		},
		{
			name: "strip prefix from Aggregate result",
			data: map[string]interface{}{
				"Aggregate": map[string]interface{}{
					"Tenanta__Articles": []interface{}{
						map[string]interface{}{"meta": map[string]interface{}{"count": 5}},
					},
				},
			},
			ns: "tenanta",
			expected: map[string]interface{}{
				"Aggregate": map[string]interface{}{
					"Articles": []interface{}{
						map[string]interface{}{"meta": map[string]interface{}{"count": 5}},
					},
				},
			},
		},
		{
			name: "multiple classes",
			data: map[string]interface{}{
				"Get": map[string]interface{}{
					"Tenanta__Articles":  []interface{}{},
					"Tenanta__BlogPosts": []interface{}{},
				},
			},
			ns: "tenanta",
			expected: map[string]interface{}{
				"Get": map[string]interface{}{
					"Articles":  []interface{}{},
					"BlogPosts": []interface{}{},
				},
			},
		},
		{
			name:     "nil data",
			data:     nil,
			ns:       "tenanta",
			expected: nil,
		},
		{
			name: "class without matching prefix left unchanged",
			data: map[string]interface{}{
				"Get": map[string]interface{}{
					"Tenantb__Articles": []interface{}{},
				},
			},
			ns: "tenanta",
			expected: map[string]interface{}{
				"Get": map[string]interface{}{
					"Tenantb__Articles": []interface{}{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &tailorincgraphql.Result{Data: tt.data}
			stripNamespacePrefixFromGraphQLResult(result, tt.ns)
			assert.Equal(t, tt.expected, result.Data)
		})
	}
}

func TestStripNamespacePrefixFromGraphQLResult_NilResult(t *testing.T) {
	// Should not panic on nil result
	stripNamespacePrefixFromGraphQLResult(nil, "tenanta")
}

func TestStripNamespacePrefixFromGraphQLResponse(t *testing.T) {
	resp := &models.GraphQLResponse{
		Data: map[string]models.JSONObject{
			"Get": map[string]interface{}{
				"Tenanta__Articles": []interface{}{
					map[string]interface{}{"title": "Hello"},
				},
			},
		},
	}

	stripNamespacePrefixFromGraphQLResponse(resp, "tenanta")

	expected := map[string]models.JSONObject{
		"Get": map[string]interface{}{
			"Articles": []interface{}{
				map[string]interface{}{"title": "Hello"},
			},
		},
	}
	assert.Equal(t, expected, resp.Data)
}

func TestStripNamespacePrefixFromGraphQLResponse_Nil(t *testing.T) {
	// Should not panic
	stripNamespacePrefixFromGraphQLResponse(nil, "tenanta")

	resp := &models.GraphQLResponse{}
	stripNamespacePrefixFromGraphQLResponse(resp, "tenanta")
}

func TestIsIntrospectionQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name:     "__schema query",
			query:    `{ __schema { types { name } } }`,
			expected: true,
		},
		{
			name:     "__schema queryType",
			query:    `{ __schema { queryType { fields { name } } } }`,
			expected: true,
		},
		{
			name:     "__type query",
			query:    `{ __type(name: "GetObjectsObj") { fields { name } } }`,
			expected: true,
		},
		{
			name:     "regular Get query",
			query:    `{ Get { Articles { title } } }`,
			expected: false,
		},
		{
			name:     "regular Aggregate query",
			query:    `{ Aggregate { Articles { meta { count } } } }`,
			expected: false,
		},
		{
			name:     "empty query",
			query:    "",
			expected: false,
		},
		{
			name:     "__schema in string value is still detected",
			query:    `{ Get { Articles(where: { path: "__schema" }) { title } } }`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isIntrospectionQuery(tt.query))
		})
	}
}
