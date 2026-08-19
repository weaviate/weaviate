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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"acceptance_tests_with_client/internal/wvhost"
)

// TestUsageWithLegacyAndNamedVectors covers a class carrying both a legacy
// class-level vector and named vectors — a supported state reached by adding
// named vectors to a class created with a legacy vector. The usage report must
// list both, the legacy one under the empty name. ExtractVectorConfigs used to
// treat the two as mutually exclusive, so cold shards dropped the legacy
// vector and its storage from the report; loaded shards enumerate their
// indexes directly and never had the gap.
func TestUsageWithLegacyAndNamedVectors(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.NoError(t, err)

	const (
		namedVector      = "named"
		legacyDimensions = 4
		namedDimensions  = 8
	)
	className := t.Name() + "Class"
	tenants := []models.Tenant{{Name: "tenant0"}, {Name: "tenant1"}}
	coldTenant := tenants[0].Name

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	// a class with both vector kinds cannot be created in one call: create it
	// with the legacy vector, then add the named vector through a class update
	class := &models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties: []*models.Property{
			{Name: "name", DataType: []string{string(schema.DataTypeText)}},
			{Name: "description", DataType: []string{string(schema.DataTypeText)}},
		},
		Vectorizer:      "none",
		VectorIndexType: "hnsw",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	class.VectorConfig = map[string]models.VectorConfig{
		namedVector: {
			Vectorizer:      map[string]any{"none": map[string]any{}},
			VectorIndexType: "hnsw",
		},
	}
	require.NoError(t, c.Schema().ClassUpdater().WithClass(class).Do(ctx))

	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	const objectCount = 5
	for _, tenant := range tenants {
		insertObjects(t, objectCount, c, className, tenant.Name,
			models.Vectors{namedVector: generateRandomVector(namedDimensions)},
			generateRandomVector(legacyDimensions))
	}

	assertBothVectorsReported := func(ct *assert.CollectT, wantShardStatus map[string]string) {
		colUsage, err := GetDebugUsageForCollection(className)
		require.NoError(ct, err)
		require.Len(ct, colUsage.Shards, len(tenants))

		for _, shard := range colUsage.Shards {
			if wantStatus, ok := wantShardStatus[shard.Name]; ok {
				require.Equal(ct, wantStatus, shard.Status)
			}
			names := make([]string, 0, len(shard.NamedVectors))
			for _, vectorUsage := range shard.NamedVectors {
				names = append(names, vectorUsage.Name)
			}
			require.Equal(ct, []string{"", namedVector}, names,
				"shard %q must report the legacy vector alongside the named one", shard.Name)

			for _, vectorUsage := range shard.NamedVectors {
				wantDimensions := namedDimensions
				if vectorUsage.Name == "" {
					wantDimensions = legacyDimensions
				}
				require.Len(ct, vectorUsage.Dimensionalities, 1, "vector %q", vectorUsage.Name)
				assert.Equal(ct, wantDimensions, vectorUsage.Dimensionalities[0].Dimensions, "vector %q", vectorUsage.Name)
				assert.Equal(ct, objectCount, vectorUsage.Dimensionalities[0].Count, "vector %q", vectorUsage.Name)
			}
		}
	}

	// all tenants hot: the loaded path enumerates shard indexes directly
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assertBothVectorsReported(ct, nil)
	}, 60*time.Second, 500*time.Millisecond)

	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: coldTenant, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

	// cold tenant: the shard is reported from disk via the schema's vector
	// configs, which must include the legacy vector
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assertBothVectorsReported(ct, map[string]string{coldTenant: "inactive"})
	}, 60*time.Second, 500*time.Millisecond)
}
