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
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"acceptance_tests_with_client/internal/wvhost"
)

// TestUsageOfConfiguredVectorWithoutData pins that a configured, never-written
// named vector reports the same ratio and dimensionality whether its tenant is
// loaded or cold. A quantizer is on from the moment its index exists, while the
// quantized vectors a cold shard looks for only appear once something is written,
// so the two paths have to agree on a vector that holds nothing.
func TestUsageOfConfiguredVectorWithoutData(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.NoError(t, err)

	const (
		populatedVector = "populated"
		emptyHNSWBQ     = "emptyHnswBq"
		emptyFlatBQ     = "emptyFlatBq"
		emptyPlain      = "emptyPlain"
		dimensions      = 8
		objectCount     = 5
	)
	className := t.Name() + "Class"
	tenantName := "tenant0"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	noVectorizer := map[string]any{"none": map[string]any{}}
	quantized := map[string]any{"bq": map[string]any{"enabled": true}}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties: []*models.Property{
			{Name: "name", DataType: []string{string(schema.DataTypeText)}},
			{Name: "description", DataType: []string{string(schema.DataTypeText)}},
		},
		VectorConfig: map[string]models.VectorConfig{
			populatedVector: {Vectorizer: noVectorizer, VectorIndexType: "hnsw"},
			emptyHNSWBQ:     {Vectorizer: noVectorizer, VectorIndexType: "hnsw", VectorIndexConfig: quantized},
			emptyFlatBQ:     {Vectorizer: noVectorizer, VectorIndexType: "flat", VectorIndexConfig: quantized},
			emptyPlain:      {Vectorizer: noVectorizer, VectorIndexType: "hnsw"},
		},
	}).Do(ctx))

	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName}).Do(ctx))

	insertObjects(t, objectCount, c, className, tenantName,
		models.Vectors{populatedVector: generateRandomVector(dimensions)}, nil)

	onlyShardUsage := func(t require.TestingT) *usagetypes.ShardUsage {
		colUsage, err := GetDebugUsageForCollection(className)
		require.NoError(t, err)
		require.Len(t, colUsage.Shards, 1)
		return colUsage.Shards[0]
	}

	loadedShard := onlyShardUsage(t)
	require.Equal(t, []string{emptyFlatBQ, emptyHNSWBQ, emptyPlain, populatedVector},
		namedVectorNames(loadedShard))
	for _, vectorUsage := range loadedShard.NamedVectors {
		if vectorUsage.Name == populatedVector {
			continue
		}
		assert.Empty(t, vectorUsage.Dimensionalities,
			"vector %q holds nothing and has no dimensionality to bill", vectorUsage.Name)
		assert.Equal(t, float64(1), vectorUsage.VectorCompressionRatio,
			"vector %q holds nothing to compress", vectorUsage.Name)
	}

	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		unloadedShard := onlyShardUsage(ct)
		require.Equal(ct, "inactive", unloadedShard.Status)
		require.NoError(ct, vectorsDifference(loadedShard.NamedVectors, unloadedShard.NamedVectors))
	}, 60*time.Second, 500*time.Millisecond)
}
