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

package rbac

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestViewerEndpointsSchema(t *testing.T) {
	endpoints := []struct {
		endpoint string
		methods  []string
		success  []bool
		body     map[string][]byte
	}{
		{endpoint: "/schema", methods: []string{"GET", "POST"}, success: []bool{true, false}, body: map[string][]byte{"POST": []byte(`{"class": "value"}`)}},
		{endpoint: "/schema/RandomClass", methods: []string{"GET", "PUT", "DELETE"}, success: []bool{true, false, false}, body: map[string][]byte{"PUT": []byte(`{"class": "value"}`)}},
		{endpoint: "/schema/RandomClass/properties", methods: []string{"POST"}, success: []bool{false}, body: map[string][]byte{"POST": []byte(`{}`)}},
	}

	for _, endpoint := range endpoints {
		if len(endpoint.methods) != len(endpoint.success) {
			t.Fatalf("expected %d methods and success, got %d", len(endpoint.methods), len(endpoint.success))
		}
		for i, method := range endpoint.methods {
			t.Run(endpoint.endpoint+method, func(t *testing.T) {
				bodyContent, hasBody := endpoint.body[method]
				var req *http.Request
				var err error
				if hasBody {
					reqBody := bytes.NewBuffer(bodyContent)
					req, err = http.NewRequest(method, "http://localhost:8081/v1"+endpoint.endpoint, reqBody)
					require.Nil(t, err)
					req.Header.Set("Content-Type", "application/json") // Set content type to JSON
				} else {
					req, err = http.NewRequest(method, "http://localhost:8081/v1"+endpoint.endpoint, nil)
					require.Nil(t, err)
				}

				// Set the Authorization header with the viewer-key
				req.Header.Set("Authorization", "Bearer viewer-key")

				// Perform the request
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("request to %s failed: %v", endpoint.endpoint, err)
				}
				defer resp.Body.Close()

				// Check if the response succeeded or failed as expected
				if endpoint.success[i] && resp.StatusCode == 403 {
					t.Errorf("expected success for %s %s, but got status %d", method, endpoint.endpoint, resp.StatusCode)
				} else if !endpoint.success[i] && resp.StatusCode != 403 {
					t.Errorf("expected failure for %s %s, but got status %d", method, endpoint.endpoint, resp.StatusCode)
				}
			})
		}
	}
}
