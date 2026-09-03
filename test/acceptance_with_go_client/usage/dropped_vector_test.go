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

package usage

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"acceptance_tests_with_client/internal/wvhost"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestUsageAfterDroppingNamedVector covers a dropped named vector aborting the
// whole node's usage report once any tenant is cold.
//
// Dropping a named vector leaves its schema entry in place with VectorIndexType
// "none" and no VectorIndexConfig. The cold path enumerates target vectors from
// that schema and asserted every entry to a concrete config type, so the nil
// config failed the assertion. The error is not fs.ErrNotExist, so the recovery
// arm in usageForShard did not absorb it and it propagated out of
// service.Usage — costing every collection's usage, not just this one.
//
// The loaded path is immune, which is why this only shows up once a tenant goes
// cold: dropVectorIndex removes the entry from the index's vector configs, so a
// loaded shard has no such vector to enumerate.
func TestUsageAfterDroppingNamedVector(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.NoError(t, err)

	const (
		keptVector    = "one"
		droppedVector = "toBeDropped"
		dimensions    = 8
	)
	className := t.Name() + "Class"
	tenants := []models.Tenant{{Name: "tenant0"}, {Name: "tenant1"}, {Name: "tenant2"}}
	coldTenant := tenants[0].Name

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	namedVector := models.VectorConfig{
		Vectorizer:      map[string]any{"none": map[string]any{}},
		VectorIndexType: "hnsw",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties: []*models.Property{
			{Name: "first", DataType: []string{string(schema.DataTypeText)}},
		},
		VectorConfig: map[string]models.VectorConfig{
			keptVector:    namedVector,
			droppedVector: namedVector,
		},
	}).Do(ctx))

	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	for i, tenant := range tenants {
		_, err := c.Data().Creator().WithClassName(className).
			WithID(uuid.NewString()).
			WithTenant(tenant.Name).
			WithProperties(map[string]any{"first": fmt.Sprintf("hello%d", i)}).
			WithVectors(models.Vectors{
				keptVector:    generateRandomVector(dimensions),
				droppedVector: generateRandomVector(dimensions),
			}).Do(ctx)
		require.NoError(t, err)
	}

	// baseline: every tenant is hot and both vectors are reported
	colUsage, err := GetDebugUsageForCollection(className)
	require.NoError(t, err)
	require.Len(t, colUsage.Shards, len(tenants))
	for _, shard := range colUsage.Shards {
		assert.Equal(t, []string{keptVector, droppedVector}, namedVectorNames(shard),
			"shard %q should report both vectors before the drop", shard.Name)
	}

	dropVectorIndex(t, wvhost.REST(), className, droppedVector)

	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: coldTenant, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

	// Without the fix the cold tenant fails the assertion on the nil config and
	// GetDebugUsageForCollection returns the resulting HTTP 400 instead
	// of a report — for the whole node, not just this collection.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		colUsage, err := GetDebugUsageForCollection(className)
		require.NoError(ct, err)
		require.Len(ct, colUsage.Shards, len(tenants))

		for _, shard := range colUsage.Shards {
			assert.Equal(ct, []string{keptVector}, namedVectorNames(shard),
				"shard %q should report only the surviving vector", shard.Name)
			if shard.Name == coldTenant {
				assert.Equal(ct, "inactive", shard.Status)
			}
		}
	}, 60*time.Second, 500*time.Millisecond)
}

// dropVectorIndex calls the experimental drop endpoint, which the go client does
// not expose.
func dropVectorIndex(t *testing.T, host, className, targetVector string) {
	t.Helper()

	url := fmt.Sprintf("http://%s/v1/schema/%s/vectors/%s/index", host, className, targetVector)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func namedVectorNames(shard *usagetypes.ShardUsage) []string {
	names := make([]string, 0, len(shard.NamedVectors))
	for _, namedVector := range shard.NamedVectors {
		names = append(names, namedVector.Name)
	}
	return names
}
