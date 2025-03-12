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

package clusterapi_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
)

func TestMaintenanceModeIndices(t *testing.T) {
	noopAuth := clusterapi.NewNoopAuthHandler()
	// NOTE leaving shards, db, and logger nil for now, fill in when needed
	indices := clusterapi.NewIndices(nil, nil, noopAuth, func() bool { return true }, nil)
	mux := http.NewServeMux()
	mux.Handle("/indices/", indices.Indices())
	server := httptest.NewServer(mux)

	defer server.Close()

	maintenanceModeExpectedHTTPStatus := http.StatusTeapot
	requestURL := func(suffix string) string {
		return fmt.Sprintf("%s/indices/MyClass/shards/myshard%s", server.URL, suffix)
	}
	indicesTestRequests := []indicesTestRequest{
		{"POST", "/objects/_search"},
		{"POST", "/objects/_find"},
		{"POST", "/objects/_aggregations"},
		{"PUT", "/objects:overwrite"},
		{"GET", "/objects:digest"},
		{"GET", "/objects/deadbeef"},
		{"DELETE", "/objects/deadbeef"},
		{"PATCH", "/objects/deadbeef"},
		{"GET", "/objects"},
		{"POST", "/objects"},
		{"DELETE", "/objects"},
		{"POST", "/references"},
		{"GET", "/queuesize"},
		{"GET", "/status"},
		{"POST", "/status"},
		{"POST", "/files/myfile"},
		{"POST", ""},
		{"PUT", ":reinit"},
	}
	for _, testRequest := range indicesTestRequests {
		t.Run(fmt.Sprintf("%s on %s returns maintenance mode status", testRequest.method, testRequest.suffix), func(t *testing.T) {
			req, err := http.NewRequest(testRequest.method, requestURL(testRequest.suffix), nil)
			assert.Nil(t, err)
			res, err := http.DefaultClient.Do(req)
			assert.Nil(t, err)
			defer res.Body.Close()
			assert.True(t, res.StatusCode == maintenanceModeExpectedHTTPStatus, "expected %d, got %d", maintenanceModeExpectedHTTPStatus, res.StatusCode)
		})
	}
}

type indicesTestRequest struct {
	method string
	suffix string
}
