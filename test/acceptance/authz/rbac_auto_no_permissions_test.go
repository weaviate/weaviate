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

func TestCollectEndpoints(t *testing.T) {
	// print all endpoints grouped  by method
	col, err := newCollector()
	require.Nil(t, err)
	col.prettyPrint()
}

func TestAuthzAllEndpointsNoPermissionDynamically(t *testing.T) {
	adminKey := "admin-key"
	customKey := "custom-key"

	compose, down := composeUpShared(t)
	defer down()

	// create class via admin
	className := "ABC"
	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}
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

	ignoreEndpoints := []string{
		"/",
		"/.well-known/live",
		"/.well-known/openid-configuration",
		"/.well-known/ready",
		"/meta",
		"/users/own-info",             // will return info for own user
		"/replication/replicate/{id}", // for the same reason as backups above
		"/replication/replicate/{id}/cancel",
		"/replication/sharding-state",
		"/tasks",                // tasks is internal endpoint
		"/classifications/{id}", // requires to get classification by id first before checking of authz permissions
		"/tokenize",             // stateless compute, no authz; POST only because it needs a body
		// MCP's HTTP methods are protocol operations (GET=SSE session, DELETE=session
		// close, POST=JSON-RPC envelope), not RBAC-gated resources. Authz runs per
		// tool-call inside the JSON-RPC handler. RBAC is covered in mcp_test.go.
		"/mcp",
		// TODO: these leak status (404 for aliases, 501 for replication) before
		// authz runs, so a caller without permission learns the resource exists.
		// Fix by moving authz to the top of each handler, then drop these entries.
		"/aliases/{aliasName}",
		"/replication/replicate",
		"/replication/replicate/force-delete",
		"/replication/replicate/list",
		"/replication/scale",
		// Export filters classes (POST) or reads metadata before authz
		// (GET/DELETE), so a caller without permission gets 422/404 instead
		// of 403. RBAC for export is covered in export_test.go.
		"/export/{backend}",
		"/export/{backend}/{id}",
		// Namespaces are disabled on this compose, so every method on the
		// per-namespace endpoint returns 404 before authz runs. RBAC for
		// namespaces is covered in the namespaces suite.
		"/namespaces/{namespace_id}",
	}

	// These leak 404 on a non-existent backup ID because the meta is read
	// before class-aware authz runs. RBAC for them is covered in backups_test.go.
	ignoreEndpointMethods := map[string]map[string]bool{
		"/backups/{backend}/{id}":         {http.MethodGet: true},
		"/backups/{backend}/{id}/restore": {http.MethodGet: true, http.MethodPost: true},
	}

	ignoreGetAll := []string{
		"/authz/roles",
		"/authz/groups/{groupType}", // returns 200 with an empty, per-group filtered list (same pattern as /authz/roles)
		"/backups/{backend}",        // returns 200 with an empty, per-backup filtered list (same pattern as /authz/roles)
		"/objects",
		"/schema",
		"/schema/{className}/tenants",
		"/schema/{className}/tenants/{tenantName}",
		"/users/db",
		"/aliases",
		"/namespaces", // returns 200 with an empty, per-caller filtered list (same pattern as /authz/roles)
	}

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().URI(), endpoint.path)
		url = strings.ReplaceAll(url, "/objects/{className}/{id}", fmt.Sprintf("/objects/%s/%s", className, UUID1.String()))
		url = strings.ReplaceAll(url, "/objects/{id}", fmt.Sprintf("/objects/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "/replication/replicate/{id}", fmt.Sprintf("/replication/replicate/%s", UUID1.String()))
		url = strings.ReplaceAll(
			url,
			"/replication/scale",
			fmt.Sprintf("/replication/scale?collection=%s&replicationFactor=%d", className, 1),
		)
		url = strings.ReplaceAll(url, "{className}", className)
		url = strings.ReplaceAll(url, "{tenantName}", "Tenant1")
		url = strings.ReplaceAll(url, "{shardName}", "Shard1")
		url = strings.ReplaceAll(url, "{id}", "admin-user")
		url = strings.ReplaceAll(url, "{backend}", "filesystem")
		url = strings.ReplaceAll(url, "{propertyName}", "someProperty")
		url = strings.ReplaceAll(url, "{indexName}", "filterable")
		url = strings.ReplaceAll(url, "{vectorIndexName}", "someVectorIndex")
		url = strings.ReplaceAll(url, "{user_id}", "admin-user")
		url = strings.ReplaceAll(url, "{userType}", "db")
		url = strings.ReplaceAll(url, "{aliasName}", "alias")
		url = strings.ReplaceAll(url, "{groupType}", "oidc")
		url = strings.ReplaceAll(url, "{namespace_id}", "someNamespace")

		t.Run(url+"("+strings.ToUpper(endpoint.method)+")", func(t *testing.T) {
			require.NotContains(t, url, "{")
			require.NotContains(t, url, "}")

			shallIgnore := slices.Contains(ignoreEndpoints, endpoint.path) ||
				(endpoint.method == http.MethodGet && slices.Contains(ignoreGetAll, endpoint.path)) ||
				ignoreEndpointMethods[endpoint.path][strings.ToUpper(endpoint.method)]
			if shallIgnore {
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

			if endpoint.method == http.MethodPost || endpoint.method == http.MethodPut || endpoint.method == http.MethodPatch || endpoint.method == http.MethodDelete {
				req, err = http.NewRequest(endpoint.method, url, bytes.NewBuffer(body))
				require.Nil(t, err)
				req.Header.Set("Content-Type", "application/json")

			} else {
				req, err = http.NewRequest(endpoint.method, url, nil)
				require.Nil(t, err)
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", customKey))
			client := &http.Client{}
			resp, err := client.Do(req)
			require.Nil(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}
}
