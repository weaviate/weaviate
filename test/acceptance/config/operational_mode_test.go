//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/usecases/config"
)

const UUID1 = strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168241")

func TestOperationalModeDynamically(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name            string
		operationalMode string
		whitelist       map[string]struct{}
		methods         []string
	}{
		{
			name:            "standard operational mode",
			operationalMode: models.NodeStatusOperationalModeReadWrite,
			whitelist:       nil,
			methods:         nil,
		},
		{
			name:            "readonly operational mode",
			operationalMode: models.NodeStatusOperationalModeReadOnly,
			whitelist:       config.ReadOnlyWhitelist,
			methods: []string{
				http.MethodGet,
				http.MethodHead,
				http.MethodOptions,
				http.MethodTrace,
			},
		},
		{
			name:            "writeonly operational mode",
			operationalMode: models.NodeStatusOperationalModeWriteOnly,
			whitelist:       config.WriteOnlyWhitelist,
			methods: []string{
				http.MethodPost,
				http.MethodPut,
				http.MethodPatch,
				http.MethodDelete,
			},
		},
		{
			name:            "scaleout operational mode",
			operationalMode: models.NodeStatusOperationalModeScaleOut,
			whitelist:       config.ScaleOutWhitelist,
			methods: []string{
				http.MethodGet,
				http.MethodHead,
				http.MethodOptions,
				http.MethodTrace,
			},
		},
	} {
		t.Run(fmt.Sprintf("testing endpoints dynamically with operational mode %s", tt.operationalMode), func(t *testing.T) {
			compose, err := docker.New().
				WithWeaviateWithGRPC().
				WithWeaviateEnv("OPERATIONAL_MODE", tt.operationalMode).
				Start(ctx)
			require.Nil(t, err)
			defer func() {
				if err := compose.Terminate(ctx); err != nil {
					t.Fatalf("failed to terminate test containers: %v", err)
				}
			}()

			col, err := newCollector()
			require.Nil(t, err)

			endpoints := col.allEndpoints()

			for _, endpoint := range endpoints {
				url := fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().GetEndpoint(docker.HTTP), endpoint.path)
				url = strings.ReplaceAll(url, "/objects/{className}/{id}", fmt.Sprintf("/objects/ABC/%s", UUID1.String()))
				url = strings.ReplaceAll(url, "/objects/{id}", fmt.Sprintf("/objects/%s", UUID1.String()))
				url = strings.ReplaceAll(url, "/replication/replicate/{id}", fmt.Sprintf("/replication/replicate/%s", UUID1.String()))
				url = strings.ReplaceAll(url, "{className}", "ABC")
				url = strings.ReplaceAll(url, "{tenantName}", "Tenant1")
				url = strings.ReplaceAll(url, "{shardName}", "Shard1")
				url = strings.ReplaceAll(url, "{id}", "someId")
				url = strings.ReplaceAll(url, "{backend}", "filesystem")
				url = strings.ReplaceAll(url, "{propertyName}", "someProperty")
				url = strings.ReplaceAll(url, "{user_id}", "admin-user")
				url = strings.ReplaceAll(url, "{userType}", "db")
				url = strings.ReplaceAll(url, "{groupType}", "oidc")
				url = strings.ReplaceAll(url, "{aliasName}", "aliasName")

				t.Run(url+"("+strings.ToUpper(endpoint.method)+")", func(t *testing.T) {
					require.NotContains(t, url, "{")
					require.NotContains(t, url, "}")

					var req *http.Request
					var err error

					endpoint.method = strings.ToUpper(endpoint.method)
					if endpoint.method == "POST" || endpoint.method == "PUT" || endpoint.method == "PATCH" || endpoint.method == "DELETE" {
						req, err = http.NewRequest(endpoint.method, url, bytes.NewBuffer(endpoint.validGeneratedBodyData))
						require.Nil(t, err)
						req.Header.Set("Content-Type", "application/json")
					} else {
						req, err = http.NewRequest(endpoint.method, url, nil)
						require.Nil(t, err)
					}

					client := &http.Client{}
					resp, err := client.Do(req)
					require.Nil(t, err)
					defer resp.Body.Close()

					if tt.methods == nil {
						require.NotEqual(t, http.StatusServiceUnavailable, resp.StatusCode)
						return
					}

					methodAllowed := false
					for _, m := range tt.methods {
						if m == endpoint.method {
							methodAllowed = true
							break
						}
					}
					if methodAllowed {
						require.NotEqual(t, http.StatusServiceUnavailable, resp.StatusCode)
						return
					}
					require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
				})
			}
		})
	}
}
