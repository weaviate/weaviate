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

package authz

import (
	"bytes"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAuthzAllEndpointsViewerDynamically(t *testing.T) {
	adminKey := "admin-key"
	viewerKey := "viewer-key"
	viewerUser := "viewer-user"

	compose, down := composeUpShared(t)
	defer down()

	// create class via admin
	className := "ABC"
	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}
	helper.AssignRoleToUser(t, adminKey, "viewer", viewerUser)

	helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))
	helper.CreateClassAuth(t, &models.Class{Class: className, MultiTenancyConfig: &models.MultiTenancyConfig{
		Enabled: true,
	}}, adminKey)
	tenants := make([]*models.Tenant, len(tenantNames))
	for i := range tenants {
		tenants[i] = &models.Tenant{Name: tenantNames[i], ActivityStatus: "HOT"}
	}
	helper.CreateTenantsAuth(t, className, tenants, adminKey)

	col, err := newCollector()
	require.Nil(t, err)

	endpoints := col.allEndpoints()

	viewerPOSTNotForbiddenEndpoints := []string{
		"/graphql",
		"/graphql/batch",
		"/objects/validate",
		"/replication/replicate/{id}", // for the same reason as backups above
		"/replication/replicate/{id}/cancel",
		"/authz/roles/{id}/has-permission", // must be a POST rather than GET or HEAD due to need of body. but viewer can access it due to its permissions
		"/tokenize",                        // stateless compute, no authz; POST only because it needs a body
		"/schema/{className}/properties/{propertyName}/tokenize", // authorizes READ, which a viewer holds
	}

	// TODO: these leak status (404 for aliases, 501 for replication) before
	// authz runs, so a viewer mutating them gets 404/501 instead of 403.
	// Fix by moving authz to the top of each handler, then drop these entries.
	ignoreEndpoints := []string{
		// MCP's HTTP methods are protocol operations (GET=SSE session, DELETE=session
		// close, POST=JSON-RPC envelope), not RBAC-gated resources. Authz runs per
		// tool-call inside the JSON-RPC handler. RBAC is covered in mcp_test.go.
		"/mcp",
		"/aliases/{aliasName}",
		"/replication/replicate",
		"/replication/replicate/force-delete",
		"/replication/scale",
		// Export filters classes (POST) or reads metadata before authz
		// (GET/DELETE), so a viewer gets 422/404 instead of 403. RBAC for
		// export is covered in export_test.go.
		"/export/{backend}",
		"/export/{backend}/{id}",
		// Namespaces are disabled on this compose, so every method on the
		// per-namespace endpoint returns 404 before authz runs. RBAC for
		// namespaces is covered in the namespaces suite.
		"/namespaces/{namespace_id}",
	}

	// Restore leaks 404 on a non-existent backup ID because the meta is read
	// before class-aware authz runs. RBAC for restore is covered in backups_test.go.
	ignoreEndpointMethods := map[string]map[string]bool{
		"/backups/{backend}/{id}/restore": {http.MethodPost: true},
	}

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().URI(), endpoint.path)
		url = strings.ReplaceAll(url, "/objects/{className}/{id}", fmt.Sprintf("/objects/%s/%s", className, UUID1.String()))
		url = strings.ReplaceAll(url, "/objects/{id}", fmt.Sprintf("/objects/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "/replication/replicate/{id}", fmt.Sprintf("/replication/replicate/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "{className}", className)
		url = strings.ReplaceAll(url, "{tenantName}", "Tenant1")
		url = strings.ReplaceAll(url, "{shardName}", "Shard1")
		url = strings.ReplaceAll(url, "{id}", "someId")
		url = strings.ReplaceAll(url, "{backend}", "filesystem")
		url = strings.ReplaceAll(url, "{propertyName}", "someProperty")
		url = strings.ReplaceAll(url, "{indexName}", "filterable")
		url = strings.ReplaceAll(url, "{vectorIndexName}", "someVectorIndex")
		url = strings.ReplaceAll(url, "{user_id}", "admin-user")
		url = strings.ReplaceAll(url, "{userType}", "db")
		url = strings.ReplaceAll(url, "{groupType}", "oidc")
		url = strings.ReplaceAll(url, "{aliasName}", "aliasName")
		url = strings.ReplaceAll(url, "{namespace_id}", "someNamespace")

		t.Run(url+"("+strings.ToUpper(endpoint.method)+")", func(t *testing.T) {
			require.NotContains(t, url, "{")
			require.NotContains(t, url, "}")

			if slices.Contains(ignoreEndpoints, endpoint.path) ||
				ignoreEndpointMethods[endpoint.path][strings.ToUpper(endpoint.method)] {
				t.Skip("Endpoint is in ignore list")
				return
			}

			var req *http.Request
			var err error

			endpoint.method = strings.ToUpper(endpoint.method)

			body := endpoint.validGeneratedBodyData
			// Backup-create's generated body is empty (schema has no type) and 422s
			// on id validation before authz; send a valid id so it reaches authz.
			if endpoint.path == "/backups/{backend}" && endpoint.method == http.MethodPost {
				body = []byte(`{"id":"someid"}`)
			}

			forbidden := false
			if endpoint.method == "POST" || endpoint.method == "PUT" || endpoint.method == "PATCH" || endpoint.method == "DELETE" {
				req, err = http.NewRequest(endpoint.method, url, bytes.NewBuffer(body))
				require.Nil(t, err)
				req.Header.Set("Content-Type", "application/json")
				forbidden = true
			} else {
				req, err = http.NewRequest(endpoint.method, url, nil)
				require.Nil(t, err)
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", viewerKey))
			client := &http.Client{}
			resp, err := client.Do(req)
			require.Nil(t, err)
			defer resp.Body.Close()

			if slices.Contains(viewerPOSTNotForbiddenEndpoints, endpoint.path) {
				require.NotEqual(t, http.StatusForbidden, resp.StatusCode)
				return
			}

			if forbidden {
				require.Equal(t, http.StatusForbidden, resp.StatusCode)
			} else {
				require.NotEqual(t, http.StatusForbidden, resp.StatusCode)
			}
		})
	}
}
