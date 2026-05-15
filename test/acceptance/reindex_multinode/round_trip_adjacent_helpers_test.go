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

package reindex_multinode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// createMTCollection creates a multi-tenant class with the given shard count.
// RF is implicitly the shard count; MT classes use the cluster's default
// replication factor for the underlying physical shards.
func createMTCollection(
	t *testing.T, restURI, className string, rf int, properties []*models.Property,
) {
	t.Helper()

	class := map[string]interface{}{
		"class":      className,
		"vectorizer": "none",
		"multiTenancyConfig": map[string]interface{}{
			"enabled": true,
		},
		"replicationConfig": map[string]interface{}{
			"factor": rf,
		},
		"properties": properties,
	}

	body, err := json.Marshal(class)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/schema", restURI),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"create MT class failed: %s", string(respBody))
}

// createTenants creates tenants on a multi-tenant class via the REST API.
func createTenants(t *testing.T, restURI, className string, tenants []string) {
	t.Helper()

	payload := make([]map[string]interface{}, 0, len(tenants))
	for _, name := range tenants {
		payload = append(payload, map[string]interface{}{
			"name":           name,
			"activityStatus": "HOT",
		})
	}

	body, err := json.Marshal(payload)
	require.NoError(t, err)

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/schema/%s/tenants", restURI, className),
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"create tenants failed: %s", string(respBody))
}

// importObjectsTenant imports the given texts into the given tenant.
func importObjectsTenant(
	t *testing.T, restURI, className, tenant string, texts []string,
) {
	t.Helper()

	for i, text := range texts {
		obj := map[string]interface{}{
			"class": className,
			"properties": map[string]interface{}{
				"text": text,
			},
			"tenant": tenant,
		}

		body, err := json.Marshal(obj)
		require.NoError(t, err)

		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/objects", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"import tenant object %d failed: %s", i, string(respBody))
	}
}

// importObjectsTwoProps imports the given texts so that both title and body
// contain the same content. This lets the same testBM25Queries probe both
// properties against the same baseline count.
func importObjectsTwoProps(t *testing.T, restURI, className string, texts []string) {
	t.Helper()

	for i, text := range texts {
		obj := map[string]interface{}{
			"class": className,
			"properties": map[string]interface{}{
				"title": text,
				"body":  text,
			},
		}

		body, err := json.Marshal(obj)
		require.NoError(t, err)

		resp, err := http.Post(
			fmt.Sprintf("http://%s/v1/objects", restURI),
			"application/json",
			bytes.NewReader(body),
		)
		require.NoError(t, err)

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"import two-prop object %d failed: %s", i, string(respBody))
	}
}

// runBM25QueryOnNodeProperty runs BM25 against a specific property on a
// specific node and returns matching object IDs.
func runBM25QueryOnNodeProperty(
	t *testing.T, restURI, className, property, query string,
) ([]string, error) {
	t.Helper()

	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}) {
				_additional { id }
			}
		}
	}`, className, query, property)

	return executeGraphQL(restURI, className, gqlQuery)
}

// runBM25QueryOnNodeTenant runs BM25 against a tenant on a specific node.
func runBM25QueryOnNodeTenant(
	t *testing.T, restURI, className, tenant, query string,
) ([]string, error) {
	t.Helper()

	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(tenant: %q, bm25: {query: %q, properties: ["text"]}) {
				_additional { id }
			}
		}
	}`, className, tenant, query)

	return executeGraphQL(restURI, className, gqlQuery)
}

// runEqualFilterOnNode runs a where Equal text filter and returns IDs.
func runEqualFilterOnNode(
	t *testing.T, restURI, className, property, value string,
) ([]string, error) {
	t.Helper()

	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {path: [%q], operator: Equal, valueText: %q}) {
				_additional { id }
			}
		}
	}`, className, property, value)

	return executeGraphQL(restURI, className, gqlQuery)
}

// executeGraphQL POSTs a GraphQL query and returns the list of _additional.id
// values from the first Get-class result set.
func executeGraphQL(restURI, className, gqlQuery string) ([]string, error) {
	reqBody := map[string]interface{}{
		"query": gqlQuery,
	}
	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/v1/graphql", restURI),
		"application/json",
		bytes.NewReader(jsonBody),
	)
	if err != nil {
		return nil, fmt.Errorf("graphql request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var gqlResp struct {
		Data struct {
			Get map[string][]map[string]interface{} `json:"Get"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &gqlResp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if len(gqlResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", gqlResp.Errors[0].Message)
	}

	items := gqlResp.Data.Get[className]
	ids := make([]string, 0, len(items))
	for _, item := range items {
		additional := item["_additional"].(map[string]interface{})
		ids = append(ids, additional["id"].(string))
	}
	return ids, nil
}

// perNodeBM25CountsProperty queries each node directly for BM25 on a
// specific property and returns the per-node match counts.
func perNodeBM25CountsProperty(
	t *testing.T, compose *docker.DockerCompose, className, property, query string,
) []int {
	t.Helper()

	counts := make([]int, 3)
	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		ids, err := runBM25QueryOnNodeProperty(t, uri, className, property, query)
		require.NoErrorf(t, err, "BM25 query prop=%s %q on node %d", property, query, i+1)
		counts[i] = len(ids)
	}
	return counts
}

// perNodeBM25CountsTenant queries each node directly for BM25 on a tenant.
func perNodeBM25CountsTenant(
	t *testing.T, compose *docker.DockerCompose, className, tenant, query string,
) []int {
	t.Helper()

	counts := make([]int, 3)
	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		ids, err := runBM25QueryOnNodeTenant(t, uri, className, tenant, query)
		require.NoErrorf(t, err, "BM25 tenant=%s query %q on node %d", tenant, query, i+1)
		counts[i] = len(ids)
	}
	return counts
}

// perNodeEqualCounts queries each node directly for a where Equal filter
// and returns the per-node match counts.
func perNodeEqualCounts(
	t *testing.T, compose *docker.DockerCompose, className, property, value string,
) []int {
	t.Helper()

	counts := make([]int, 3)
	for i := 0; i < 3; i++ {
		uri := compose.GetWeaviateNode(i + 1).URI()
		ids, err := runEqualFilterOnNode(t, uri, className, property, value)
		require.NoErrorf(t, err, "Equal filter %s=%q on node %d", property, value, i+1)
		counts[i] = len(ids)
	}
	return counts
}
