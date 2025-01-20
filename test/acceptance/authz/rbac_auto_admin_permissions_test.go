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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAuthzAllEndpointsAdminDynamically(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"

	compose, down := composeUp(t, map[string]string{adminUser: adminKey}, nil, nil)
	defer down()

	var endpointStats endpointStatsSlice

	containers := compose.Containers()
	require.Len(t, containers, 1) // started only one node
	container := containers[0].Container()

	className := "ABC"
	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}
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

	lastLogPosition := 0
	getNewAuthZLogs(t, container, &lastLogPosition) // startup logs that are irrelevant

	for _, endpoint := range endpoints {
		url := fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().URI(), endpoint.path)
		url = strings.ReplaceAll(url, "/objects/{className}/{id}", fmt.Sprintf("/objects/%s/%s", className, UUID1.String()))
		url = strings.ReplaceAll(url, "/objects/{id}", fmt.Sprintf("/objects/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "{className}", className)
		url = strings.ReplaceAll(url, "{tenantName}", "Tenant1")
		url = strings.ReplaceAll(url, "{shardName}", "Shard1")
		url = strings.ReplaceAll(url, "{id}", "someId")
		url = strings.ReplaceAll(url, "{backend}", "filesystem")
		url = strings.ReplaceAll(url, "{propertyName}", "someProperty")

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

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", adminKey))
			client := &http.Client{}
			resp, err := client.Do(req)
			require.Nil(t, err)
			defer resp.Body.Close()

			require.NotEqual(t, http.StatusForbidden, resp.StatusCode)

			authZlogs := getNewAuthZLogs(t, container, &lastLogPosition)
			// log.Println("endpoint", endpoint.method, url, "with ", len(authZlogs), ":", authZlogs)
			endpointStats = append(endpointStats, endpointStat{
				Count:    len(authZlogs),
				Method:   endpoint.method,
				Logs:     authZlogs,
				Endpoint: url,
			})
		})
	}

	// sort by number of authZ calls and append to log
	sort.Sort(endpointStats)
	t.Log("EndpointStats:", endpointStats)
}

// getNewLogs count how many log entries a given endpoint produces. If there is a huge amount, it can indicate a
// problem
func getNewAuthZLogs(t *testing.T, container testcontainers.Container, lastLogPosition *int) []string {
	logs, err := container.Logs(context.Background())
	require.Nil(t, err)
	defer logs.Close()

	scanner := bufio.NewScanner(logs)
	currentPosition := 0

	var newLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if currentPosition >= *lastLogPosition && strings.Contains(line, `"action":"authorize"`) {
			newLines = append(newLines, line)
		}
		currentPosition++
	}

	*lastLogPosition = currentPosition

	return newLines
}

type endpointStat struct {
	Count    int
	Endpoint string
	Method   string
	Logs     []string
}

func (e endpointStat) String() string {
	return fmt.Sprintf("%s %s (count: %d), Logs: %v", e.Method, e.Endpoint, e.Count, e.Logs)
}

type endpointStatsSlice []endpointStat

// Implement sort.Interface
func (e endpointStatsSlice) Len() int           { return len(e) }
func (e endpointStatsSlice) Less(i, j int) bool { return e[i].Count < e[j].Count }
func (e endpointStatsSlice) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

func (e endpointStatsSlice) String() string {
	var str string
	for _, e := range e {
		str += e.String() + "\n"
	}
	return str
}
