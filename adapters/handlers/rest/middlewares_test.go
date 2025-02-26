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

package rest

import (
	"net/http"
	"testing"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
)

func Test_staticRoute(t *testing.T) {
	spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
	require.NoError(t, err)

	api := operations.NewWeaviateAPI(spec)
	api.Init()

	router := middleware.DefaultRouter(spec, api)
	ctx := middleware.NewRoutableContext(spec, api, router)

	cases := []struct {
		name     string
		req      *http.Request
		expected string
	}{
		{
			name:     "unmatched route",
			req:      newRequest(t, "/foo"), // un-matched route
			expected: "/foo",
		},
		{
			name:     "matched route",
			req:      newRequest(t, "/v1/schema"), // matched route
			expected: "/v1/schema",
		},
		{
			name:     "matched route with dynamic path",
			req:      newRequest(t, "/v1/schema/Movies/"), // matched route.
			expected: "/v1/schema/{className}",            // yay!
		},
		{
			name:     "matched route with dynamic path 2",
			req:      newRequest(t, "/v1/schema/Movies/shards"), // matched route.
			expected: "/v1/schema/{className}/shards",           // yay!
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := staticRoute(ctx)(tc.req)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func newRequest(t *testing.T, path string) *http.Request {
	t.Helper()

	r, err := http.NewRequest("GET", path, nil)
	require.NoError(t, err)
	return r
}
