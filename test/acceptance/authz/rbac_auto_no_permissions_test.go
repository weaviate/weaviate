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
	adminUser := "admin-user"
	customKey := "custom-key"
	customUser := "custom-user"

	compose, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
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
		"/users/own-info",    // will return info for own user
		"/backups/{backend}", // we ignore backup because there is multiple endpoints doesn't need authZ and many validations
		"/backups/{backend}/{id}",
		"/backups/{backend}/{id}/restore",
		"/replication/replicate/{id}", // for the same reason as backups above
		"/replication/replicate/{id}/cancel",
		"/replication/sharding-state",
		"/tasks",                // tasks is internal endpoint
		"/classifications/{id}", // requires to get classification by id first before checking of authz permissions
	}

	ignoreGetAll := []string{
		"/authz/roles",
		"/objects",
		"/schema",
		"/schema/{className}/tenants",
		"/schema/{className}/tenants/{tenantName}",
		"/users/db",
	}

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().URI(), endpoint.path)
		url = strings.ReplaceAll(url, "/objects/{className}/{id}", fmt.Sprintf("/objects/%s/%s", className, UUID1.String()))
		url = strings.ReplaceAll(url, "/objects/{id}", fmt.Sprintf("/objects/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "/replication/replicate/{id}", fmt.Sprintf("/replication/replicate/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "{className}", className)
		url = strings.ReplaceAll(url, "{tenantName}", "Tenant1")
		url = strings.ReplaceAll(url, "{shardName}", "Shard1")
		url = strings.ReplaceAll(url, "{id}", "admin-user")
		url = strings.ReplaceAll(url, "{backend}", "filesystem")
		url = strings.ReplaceAll(url, "{propertyName}", "someProperty")
		url = strings.ReplaceAll(url, "{user_id}", "admin-user")
		url = strings.ReplaceAll(url, "{userType}", "db")

		t.Run(url+"("+strings.ToUpper(endpoint.method)+")", func(t *testing.T) {
			require.NotContains(t, url, "{")
			require.NotContains(t, url, "}")

			shallIgnore := slices.Contains(ignoreEndpoints, endpoint.path) ||
				(endpoint.method == http.MethodGet && slices.Contains(ignoreGetAll, endpoint.path))
			if shallIgnore {
				t.Skip("Endpoint is in ignore list")
				return
			}

			var req *http.Request
			var err error

			endpoint.method = strings.ToUpper(endpoint.method)

			if endpoint.method == http.MethodPost || endpoint.method == http.MethodPut || endpoint.method == http.MethodPatch || endpoint.method == http.MethodDelete {
				req, err = http.NewRequest(endpoint.method, url, bytes.NewBuffer(endpoint.validGeneratedBodyData))
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
