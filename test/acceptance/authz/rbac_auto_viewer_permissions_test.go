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
	}

	// TODO: needs to be removed and endpoints adapted — these currently leak
	// status (404 for aliases, 501 for replication) before authz runs, so a
	// viewer hitting a mutating method gets 404/501 instead of 403. Fix by
	// moving authz to the top of each handler/manager (same pattern as the
	// backup scheduler), then drop these entries.
	ignoreEndpoints := []string{
		"/aliases/{aliasName}",
		"/replication/replicate",
		"/replication/replicate/force-delete",
		"/replication/scale",
	}

	// Per-method ignore: restore on a non-existent backup ID leaks 404
	// because the meta file is read before class-aware authz can run.
	// Class-specific RBAC for restore is covered in backups_test.go.
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
		url = strings.ReplaceAll(url, "{user_id}", "admin-user")
		url = strings.ReplaceAll(url, "{userType}", "db")
		url = strings.ReplaceAll(url, "{groupType}", "oidc")
		url = strings.ReplaceAll(url, "{aliasName}", "aliasName")

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
			forbidden := false
			if endpoint.method == "POST" || endpoint.method == "PUT" || endpoint.method == "PATCH" || endpoint.method == "DELETE" {
				req, err = http.NewRequest(endpoint.method, url, bytes.NewBuffer(endpoint.validGeneratedBodyData))
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
