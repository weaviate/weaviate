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

package authz

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/test/helper"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAuthzViewerEndpoints(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"
	viewerKey := "viewer-key"
	viewerUser := "viewer-user"

	compose, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, map[string]string{viewerUser: viewerKey})
	defer down()

	weaviateUrl := compose.GetWeaviate().URI()

	uri := strfmt.URI("weaviate://localhost/Class/" + uuid.New().String())
	helper.AssignRoleToUser(t, adminKey, "viewer", viewerUser)

	endpoints := []struct {
		endpoint string
		methods  []string
		success  []bool
		arrayReq bool
		body     map[string][]byte
	}{
		{endpoint: "authz/roles", methods: []string{"GET", "POST"}, success: []bool{true, false}, arrayReq: false, body: map[string][]byte{"POST": []byte("{\"name\": \"n\", \"permissions\":[{\"action\": \"read_cluster\", \"cluster\": {}}]}")}},
		{endpoint: "authz/roles/id", methods: []string{"GET", "DELETE"}, success: []bool{true, false}, arrayReq: false},
		{endpoint: "authz/roles/id/users", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "authz/users/id/roles", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "authz/users/id/assign", methods: []string{"POST"}, success: []bool{false}, arrayReq: false, body: map[string][]byte{"POST": []byte("{\"roles\": [\"abc\"]}")}},
		{endpoint: "authz/users/id/revoke", methods: []string{"POST"}, success: []bool{false}, arrayReq: false, body: map[string][]byte{"POST": []byte("{\"roles\": [\"abc\"]}")}},
		{endpoint: "batch/objects", methods: []string{"POST", "DELETE"}, success: []bool{false, false}, arrayReq: false, body: map[string][]byte{"POST": []byte("{\"objects\": [{\"class\": \"c\"}]}")}},
		{endpoint: "batch/references", methods: []string{"POST"}, success: []bool{false}, arrayReq: true, body: map[string][]byte{"POST": []byte(fmt.Sprintf("[{\"from\": %q, \"to\": %q}]", uri+"/ref", uri))}},
		{endpoint: "classifications", methods: []string{"POST"}, success: []bool{false}, arrayReq: false},
		{endpoint: "classifications/id", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "cluster/statistics", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "graphql", methods: []string{"POST"}, success: []bool{true}, arrayReq: false},
		{endpoint: "objects", methods: []string{"GET", "POST"}, success: []bool{true, false}, arrayReq: false},
		{endpoint: "objects/" + UUID1.String(), methods: []string{"GET", "HEAD", "DELETE", "PATCH", "PUT"}, success: []bool{true, true, false, false, false}, arrayReq: false, body: map[string][]byte{"PATCH": []byte(fmt.Sprintf("{\"class\": \"c\", \"id\":%q}", UUID1.String()))}},
		{endpoint: "objects/RandomClass/" + UUID1.String(), methods: []string{"GET", "HEAD", "DELETE", "PATCH", "PUT"}, success: []bool{true, true, false, false, false}, arrayReq: false, body: map[string][]byte{"PATCH": []byte(fmt.Sprintf("{\"class\": \"c\", \"id\":%q}", UUID1.String()))}},
		{endpoint: "objects/" + UUID1.String() + "/references/prop", methods: []string{"DELETE", "POST"}, success: []bool{false, false}, arrayReq: false},
		{endpoint: "objects/" + UUID1.String() + "/references/prop", methods: []string{"PUT"}, success: []bool{false}, arrayReq: true},
		{endpoint: "objects/RandomClass/" + UUID1.String() + "/references/prop", methods: []string{"DELETE", "POST"}, success: []bool{false, false}, arrayReq: false},
		{endpoint: "objects/RandomClass/" + UUID1.String() + "/references/prop", methods: []string{"PUT"}, success: []bool{false}, arrayReq: true},
		{endpoint: "objects/validate", methods: []string{"POST"}, success: []bool{true}, arrayReq: false},
		{endpoint: "meta", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "nodes", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "schema", methods: []string{"GET", "POST"}, success: []bool{true, false}, arrayReq: false},
		{endpoint: "schema/RandomClass", methods: []string{"GET", "PUT", "DELETE"}, success: []bool{true, false, false}, arrayReq: false},
		{endpoint: "schema/RandomClass/properties", methods: []string{"POST"}, success: []bool{false}, arrayReq: false},
		{endpoint: "schema/RandomClass/shards", methods: []string{"GET"}, success: []bool{true}, arrayReq: false},
		{endpoint: "schema/RandomClass/shards/name", methods: []string{"PUT"}, success: []bool{false}, arrayReq: false},
		{endpoint: "schema/RandomClass/tenants", methods: []string{"GET", "POST", "PUT", "DELETE"}, success: []bool{true, false, false, false}, arrayReq: true},
		{endpoint: "schema/RandomClass/tenants/name", methods: []string{"HEAD"}, success: []bool{true}, arrayReq: false},
	}

	for _, endpoint := range endpoints {
		if len(endpoint.methods) != len(endpoint.success) {
			t.Fatalf("expected %d methods and success, got %d", len(endpoint.methods), len(endpoint.success))
		}
		for i, method := range endpoint.methods {
			t.Run(endpoint.endpoint+"_"+method, func(t *testing.T) {
				var req *http.Request
				var err error
				if method == "POST" || method == "PUT" || method == "PATCH" || method == "DELETE" {
					var body []byte
					if bodyC, ok := endpoint.body[method]; ok {
						body = bodyC
					} else {
						if endpoint.arrayReq {
							body = []byte(`[]`)
						} else {
							body = []byte(`{}`)
						}
					}

					reqBody := bytes.NewBuffer(body)
					req, err = http.NewRequest(method, fmt.Sprintf("http://%s/v1/%s", weaviateUrl, endpoint.endpoint), reqBody)
					require.Nil(t, err)
					req.Header.Set("Content-Type", "application/json")
				} else if method == "DELETE" {
					reqBody := strings.NewReader("[\n  \"\"\n]")
					req, err = http.NewRequest(method, fmt.Sprintf("http://%s/v1/%s", weaviateUrl, endpoint.endpoint), reqBody)
					require.Nil(t, err)
					req.Header.Set("Content-Type", "application/json")

				} else {
					req, err = http.NewRequest(method, fmt.Sprintf("http://%s/v1/%s", weaviateUrl, endpoint.endpoint), nil)
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
