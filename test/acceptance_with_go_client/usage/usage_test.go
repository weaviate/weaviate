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

package usage

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"golang.org/x/sync/errgroup"
)

func TestTenantStatusChanges(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := t.Name() + "Class"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 10)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	// add some data
	for i, tenant := range tenants {
		_, err := c.Data().Creator().WithClassName(className).
			WithTenant(tenant.Name).
			WithProperties(map[string]interface{}{
				"first": fmt.Sprintf("hello%d", i),
			}).Do(ctx)
		require.NoError(t, err)
	}
	endUsage := atomic.Bool{}

	go func() {
		for {
			if endUsage.Load() {
				return
			}

			usage, err := getDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			require.Equal(t, len(usage.Shards), len(tenants))

			require.NotNil(t, usage.UniqueShardCount)
			require.Equal(t, len(usage.Shards), *usage.UniqueShardCount)

			names := make(map[string]struct{})
			for _, shard := range usage.Shards {
				require.NotNil(t, shard.Name)
				if _, ok := names[*shard.Name]; ok {
					require.Fail(t, "duplicate shard name found")
				}
				names[*shard.Name] = struct{}{}
			}
			require.Equal(t, len(names), len(tenants))
		}
	}()

	var eg errgroup.Group
	for i := range tenants {
		eg.Go(
			func() error {
				require.NoError(t,
					c.Schema().TenantsUpdater().WithClassName(className).WithTenants(models.Tenant{Name: fmt.Sprintf("tenant%d", i), ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx),
				)
				require.NoError(t,
					c.Schema().TenantsUpdater().WithClassName(className).WithTenants(models.Tenant{Name: fmt.Sprintf("tenant%d", i), ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx),
				)
				return nil
			},
		)
	}
	require.NoError(t, eg.Wait())
	endUsage.Store(true)
}

func TestUsageTenantDelete(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := t.Name() + "Class"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 100)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	// add some data
	for i, tenant := range tenants {
		_, err := c.Data().Creator().WithClassName(className).
			WithTenant(tenant.Name).
			WithProperties(map[string]interface{}{
				"first": fmt.Sprintf("hello%d", i),
			}).Do(ctx)
		require.NoError(t, err)
	}

	endUsage := atomic.Bool{}
	deletedTenants := atomic.Int32{}
	go func() {
		for {
			if endUsage.Load() {
				return
			}
			deletedTenantsBeforeCall := deletedTenants.Load()
			usage, err := getDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			deletedTenantsAfterCall := deletedTenants.Load()

			// we add a bit of wiggle room here as the usage endpoint might take a bit to reflect the changes
			require.LessOrEqual(t, len(usage.Shards), len(tenants)-int(deletedTenantsBeforeCall)+1)
			require.GreaterOrEqual(t, len(usage.Shards), len(tenants)-int(deletedTenantsAfterCall)-1)

			if len(usage.Shards) > 0 {
				require.NotNil(t, usage.UniqueShardCount)
				require.Equal(t, len(usage.Shards), *usage.UniqueShardCount)
			}

			names := make(map[string]struct{})
			for _, shard := range usage.Shards {
				require.NotNil(t, shard.Name)
				if _, ok := names[*shard.Name]; ok {
					require.Fail(t, "duplicate shard name found")
				}
				names[*shard.Name] = struct{}{}
			}
		}
	}()

	for i := range tenants {
		err := c.Schema().TenantsDeleter().WithClassName(className).WithTenants(tenants[i].Name).Do(ctx)
		require.NoError(t, err)
		deletedTenants.Add(1)
	}
	endUsage.Store(true)
}

func TestCollectionDeletion(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	getClassName := func(t *testing.T, i int) string {
		return t.Name() + "Class" + fmt.Sprintf("%d", i)
	}
	numClasses := 100

	c.Schema().AllDeleter().Do(ctx)
	classCreator := c.Schema().ClassCreator()
	// create a bunch of classes
	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)

		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "first",
					DataType: []string{string(schema.DataTypeText)},
				},
			},
		}
		require.NoError(t, classCreator.WithClass(class).Do(ctx))
	}

	endUsage := atomic.Bool{}
	deletedClasses := atomic.Int32{}
	go func() {
		for {
			if endUsage.Load() {
				return
			}
			deletedClassesBeforeCall := deletedClasses.Load()
			usage, err := getDebugUsage()
			require.NoError(t, err)
			require.NotNil(t, usage)
			deletedClassesAfterCall := deletedClasses.Load()

			// we add a bit of wiggle room here as the usage endpoint might take a bit to reflect the changes
			require.LessOrEqual(t, len(usage.Collections), numClasses-int(deletedClassesBeforeCall)+1)
			require.GreaterOrEqual(t, len(usage.Collections), numClasses-int(deletedClassesAfterCall)-1)
		}
	}()

	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)
		require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))
		deletedClasses.Add(1)
	}
	endUsage.Store(true)
}

func TestRestart(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("TRACK_VECTOR_DIMENSIONS", "true").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true"). // lazy shards are shown as inactive, which would break the test
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	rest := compose.GetWeaviate().URI()
	debug := compose.GetWeaviate().DebugURI()

	c, err := client.NewClient(client.Config{Scheme: "http", Host: rest})
	require.NoError(t, err)

	className := t.Name() + "Class"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 100)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	// add some data
	for i, tenant := range tenants {
		objs := make([]*models.Object, 10)
		for j := range objs {
			vector := make([]float32, 128)
			for k := range vector {
				vector[k] = float32(i+j+k) / 10000.0
			}
			objs[j] = &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"first": fmt.Sprintf("hello%d-%d", i, j),
				},
				Vector: vector,
				Tenant: tenant.Name,
			}
		}

		_, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
		require.NoError(t, err)
	}

	usage, err := getDebugUsageWithPort(debug)
	require.NoError(t, err)
	require.NotNil(t, usage)

	require.NoError(t, compose.Stop(ctx, compose.GetWeaviate().Name(), nil))

	err = compose.Start(ctx, compose.GetWeaviate().Name())
	require.NoError(t, err)

	usage2, err := getDebugUsageWithPort(compose.GetWeaviate().DebugURI())
	require.NoError(t, err)
	require.NotNil(t, usage2)
	require.NoError(t, ReportsDifference(usage, usage2))
}

func testAllObjectsIndexed(t *testing.T, c *client.Client, className string) {
	// wait for all of the objects to get indexed
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := c.Cluster().NodesStatusGetter().
			WithClass(className).
			WithOutput("verbose").
			Do(context.Background())
		require.NoError(ct, err)
		require.NotEmpty(ct, resp.Nodes)
		for _, n := range resp.Nodes {
			require.NotEmpty(ct, n.Shards)
			for _, s := range n.Shards {
				assert.Equal(ct, "READY", s.VectorIndexingStatus)
			}
		}
	}, 30*time.Second, 500*time.Millisecond)
}

func generateRandomVector(dimensionality int) []float32 {
	if dimensionality <= 0 {
		return nil
	}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	slice := make([]float32, dimensionality)
	for i := range slice {
		slice[i] = r.Float32()
	}
	return slice
}

func insertObjects(t *testing.T, n int, c *client.Client, className, tenant string, vectors models.Vectors, vector models.C11yVector) {
	objs := []*models.Object{}
	for i := range n {
		obj := &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.NewString()),
			Properties: map[string]any{
				"name":        fmt.Sprintf("name %v", i),
				"description": fmt.Sprintf("some description %v", i),
			},
			Vectors: vectors,
			Vector:  vector,
		}
		if tenant != "" {
			obj.Tenant = tenant
		}
		objs = append(objs, obj)
	}
	resp, err := c.Batch().ObjectsBatcher().
		WithObjects(objs...).
		Do(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, resp)
	for _, r := range resp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		assert.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *r.Result.Status)
	}
}

func TestUsageWithDynamicIndex(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("TRACK_VECTOR_DIMENSIONS", "true").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true"). // lazy shards are shown as inactive, which would break the test
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "1s").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	rest := compose.GetWeaviate().URI()
	debug := compose.GetWeaviate().DebugURI()

	c, err := client.NewClient(client.Config{Scheme: "http", Host: rest})
	require.NoError(t, err)

	dynamic1024 := "dynamic1024"
	dimensions := 1024
	flat := "flat"
	bq := "bq"
	hnsw := "hnsw"
	pq := "pq"

	objectCount1 := 1000
	objectCount2 := 2000

	targetVectorDimensions := map[string]int{
		dynamic1024: dimensions,
	}

	dynamicVectorIndexConfig := map[string]any{
		"threshold": 1001,
		hnsw: map[string]any{
			pq: map[string]any{
				"enabled":       true,
				"trainingLimit": float64(100),
			},
		},
		flat: map[string]any{
			bq: map[string]any{
				"enabled": true,
			},
		},
	}

	t.Run("single tenant", func(t *testing.T) {
		className := sanitizeName("Class" + t.Name())

		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "description", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				dynamic1024: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType:   "dynamic",
					VectorIndexConfig: dynamicVectorIndexConfig,
				},
			},
		}

		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		insertObjects(t, 1000, c, className, "", models.Vectors{
			dynamic1024: generateRandomVector(targetVectorDimensions[dynamic1024]),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		colUsage, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colUsage)

		require.Len(t, colUsage.Shards, 1)
		shard := colUsage.Shards[0]
		require.Equal(t, &objectCount1, shard.ObjectsCount)
		require.Len(t, shard.NamedVectors, 1)
		require.Equal(t, dynamic1024, *shard.NamedVectors[0].Name)
		require.Equal(t, flat, *shard.NamedVectors[0].VectorIndexType)
		require.Equal(t, bq, *shard.NamedVectors[0].Compression)
		require.NotEmpty(t, shard.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shard.NamedVectors[0].Dimensionalities)
		require.NotNil(t, shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.NotNil(t, shard.NamedVectors[0].Dimensionalities[0].Count)
		require.Equal(t, dimensions, *shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, *shard.NamedVectors[0].Dimensionalities[0].Count)

		insertObjects(t, 1000, c, className, "", models.Vectors{
			dynamic1024: generateRandomVector(targetVectorDimensions[dynamic1024]),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			colUsageHnsw, err := getDebugUsageWithPortAndCollection(debug, className)
			require.NoError(ct, err)
			require.NotNil(ct, colUsageHnsw)

			require.Len(ct, colUsageHnsw.Shards, 1)
			shardHnsw := colUsageHnsw.Shards[0]
			require.NotNil(ct, shardHnsw.ObjectsCount)
			require.Equal(ct, objectCount2, *shardHnsw.ObjectsCount)
			require.Len(ct, shardHnsw.NamedVectors, 1)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Name)
			require.NotNil(ct, shardHnsw.NamedVectors[0].VectorIndexType)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Compression)
			require.Equal(ct, dynamic1024, *shardHnsw.NamedVectors[0].Name)
			require.Equal(ct, hnsw, *shardHnsw.NamedVectors[0].VectorIndexType)
			require.NotEmpty(ct, shardHnsw.NamedVectors[0].IsDynamic)
			require.True(ct, *shardHnsw.NamedVectors[0].IsDynamic)
			require.Equal(ct, pq, *shardHnsw.NamedVectors[0].Compression)
			require.NotEmpty(ct, shardHnsw.NamedVectors[0].Dimensionalities)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Dimensionalities[0].Dimensions)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Dimensionalities[0].Count)
			require.Equal(ct, dimensions, *shardHnsw.NamedVectors[0].Dimensionalities[0].Dimensions)
			require.Equal(ct, objectCount2, *shardHnsw.NamedVectors[0].Dimensionalities[0].Count)
		}, 5*time.Minute, 500*time.Millisecond)
	})

	t.Run("multi tenant", func(t *testing.T) {
		className := sanitizeName("Class" + t.Name())
		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				dynamic1024: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType:   "dynamic",
					VectorIndexConfig: dynamicVectorIndexConfig,
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}

		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		tenantName := "tenant"
		c.Schema().TenantsCreator().WithClassName(className).WithTenants(models.Tenant{Name: "tenant"}).Do(ctx)

		insertObjects(t, 1000, c, className, tenantName, models.Vectors{
			dynamic1024: generateRandomVector(targetVectorDimensions[dynamic1024]),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		colHot, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colHot)

		require.Len(t, colHot.Shards, 1)
		shardHot := colHot.Shards[0]
		require.Equal(t, &objectCount1, shardHot.ObjectsCount)
		require.Len(t, shardHot.NamedVectors, 1)
		require.Equal(t, dynamic1024, *shardHot.NamedVectors[0].Name)
		require.Equal(t, flat, *shardHot.NamedVectors[0].VectorIndexType)
		require.Equal(t, bq, *shardHot.NamedVectors[0].Compression)
		require.NotEmpty(t, shardHot.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shardHot.NamedVectors[0].Dimensionalities)
		require.NotNil(t, shardHot.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.NotNil(t, shardHot.NamedVectors[0].Dimensionalities[0].Count)
		require.Equal(t, dimensions, *shardHot.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, *shardHot.NamedVectors[0].Dimensionalities[0].Count)

		require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

		colCold, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colCold)

		require.Len(t, colCold.Shards, 1)
		shardCold := colCold.Shards[0]
		require.Len(t, shardCold.NamedVectors, 1)
		require.Equal(t, dynamic1024, *shardCold.NamedVectors[0].Name)
		// cold dynamic indices cannot easily determine the index type and compression
		require.Nil(t, shardCold.NamedVectors[0].VectorIndexType)
		require.Nil(t, shardCold.NamedVectors[0].Compression)
		require.NotNil(t, shardCold.NamedVectors[0].IsDynamic)
		require.True(t, *shardCold.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shardCold.NamedVectors[0].Dimensionalities)
		require.NotNil(t, shardCold.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.NotNil(t, shardCold.NamedVectors[0].Dimensionalities[0].Count)
		require.Equal(t, dimensions, *shardCold.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, *shardCold.NamedVectors[0].Dimensionalities[0].Count)
	})

	t.Run("legacy vectorConfig", func(t *testing.T) {
		className := sanitizeName("Class" + t.Name())

		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "description", DataType: []string{schema.DataTypeText.String()},
				},
			},
			Vectorizer:        "none",
			VectorIndexConfig: dynamicVectorIndexConfig,
			VectorIndexType:   "dynamic",
		}

		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		insertObjects(t, 1000, c, className, "", nil, generateRandomVector(targetVectorDimensions[dynamic1024]))
		testAllObjectsIndexed(t, c, className)

		colUsage, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colUsage)

		require.Len(t, colUsage.Shards, 1)
		shard := colUsage.Shards[0]
		require.Equal(t, &objectCount1, shard.ObjectsCount)
		require.Len(t, shard.NamedVectors, 1)
		require.Equal(t, flat, *shard.NamedVectors[0].VectorIndexType)
		require.Equal(t, bq, *shard.NamedVectors[0].Compression)
		require.NotEmpty(t, shard.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shard.NamedVectors[0].Dimensionalities)
		require.NotNil(t, shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.NotNil(t, shard.NamedVectors[0].Dimensionalities[0].Count)
		require.Equal(t, dimensions, *shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, *shard.NamedVectors[0].Dimensionalities[0].Count)
	})

	t.Run("storage size", func(t *testing.T) {
		classNameHnsw := sanitizeName("Class" + t.Name() + "hnsw")
		classNameFlat := sanitizeName("Class" + t.Name() + "flat")
		classNameDynamic := sanitizeName("Class" + t.Name() + "dyna") // same length

		hnsw1024 := "hnsw1024"
		flat1024 := "flat1024"

		classDynamic := &models.Class{
			Class: classNameDynamic,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				dynamic1024: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType:   "dynamic",
					VectorIndexConfig: dynamicVectorIndexConfig,
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}

		classFlat := &models.Class{
			Class: classNameFlat,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				flat1024: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType:   "flat",
					VectorIndexConfig: dynamicVectorIndexConfig[flat],
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}

		classHnsw := &models.Class{
			Class: classNameHnsw,
			Properties: []*models.Property{
				{
					Name: "name", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				hnsw1024: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexConfig: dynamicVectorIndexConfig[hnsw],
					VectorIndexType:   "hnsw",
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}

		tenant := "tenant"
		for _, class := range []*models.Class{classDynamic, classFlat, classHnsw} {
			c.Schema().ClassDeleter().WithClassName(class.Class).Do(ctx)
			defer c.Schema().ClassDeleter().WithClassName(class.Class).Do(ctx) // intended to run at the end

			require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
			require.NoError(t, c.Schema().TenantsCreator().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant}).Do(ctx))

			vectors := generateRandomVector(targetVectorDimensions[dynamic1024])
			key := ""
			for k := range class.VectorConfig {
				key = k
			}
			require.NotEqual(t, key, "")

			insertObjects(t, objectCount1, c, class.Class, tenant, models.Vectors{
				key: vectors,
			}, nil)
			testAllObjectsIndexed(t, c, class.Class)
		}

		// deactivate and activate to flush data to disk and have it comparable
		for _, class := range []*models.Class{classDynamic, classFlat, classHnsw} {
			require.NoError(t,
				c.Schema().TenantsUpdater().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx),
			)
			require.NoError(t,
				c.Schema().TenantsUpdater().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx),
			)
		}

		// before upgrade, compare dynamic and flat
		colFlat, err := getDebugUsageWithPortAndCollection(debug, classNameFlat)
		require.NoError(t, err)
		require.NotNil(t, colFlat)
		require.Len(t, colFlat.Shards, 1)
		shardFlat := colFlat.Shards[0]
		require.Equal(t, &objectCount1, shardFlat.ObjectsCount)
		require.Len(t, shardFlat.NamedVectors, 1)
		vectorFlat := shardFlat.NamedVectors[0]

		colDynamic, err := getDebugUsageWithPortAndCollection(debug, classNameDynamic)
		require.NoError(t, err)
		require.NotNil(t, colDynamic)
		shardDynamic := colDynamic.Shards[0]
		require.Equal(t, &objectCount1, shardDynamic.ObjectsCount)
		require.Len(t, shardDynamic.NamedVectors, 1)
		vectorDynamic := shardDynamic.NamedVectors[0]

		require.InDelta(t, *shardDynamic.ObjectsStorageBytes, *shardFlat.ObjectsStorageBytes, float64(*shardDynamic.ObjectsStorageBytes)*0.05)
		require.Equal(t, *shardDynamic.VectorStorageBytes, *shardFlat.VectorStorageBytes)
		require.Equal(t, vectorDynamic.Dimensionalities, vectorFlat.Dimensionalities)

		// now upgrade to hnsw and compare again
		for _, class := range []*models.Class{classDynamic, classFlat, classHnsw} {
			vectors := generateRandomVector(targetVectorDimensions[dynamic1024])
			key := ""
			for k := range class.VectorConfig {
				key = k
			}
			require.NotEqual(t, key, "")

			insertObjects(t, objectCount2-objectCount1+10, c, class.Class, tenant, models.Vectors{
				key: vectors,
			}, nil)
			testAllObjectsIndexed(t, c, class.Class)
		}

		// this will block until the index has been switched to hnsw
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			colUsageHnsw, err := getDebugUsageWithPortAndCollection(debug, classNameDynamic)
			require.NoError(ct, err)
			require.NotNil(ct, colUsageHnsw)

			require.Len(ct, colUsageHnsw.Shards, 1)
			shardHnsw := colUsageHnsw.Shards[0]
			require.Len(ct, shardHnsw.NamedVectors, 1)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Name)
			require.NotNil(ct, shardHnsw.NamedVectors[0].VectorIndexType)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Compression)
			require.Equal(ct, dynamic1024, *shardHnsw.NamedVectors[0].Name)
			require.Equal(ct, hnsw, *shardHnsw.NamedVectors[0].VectorIndexType)
		}, 5*time.Minute, 500*time.Millisecond)

		// deactivate and activate to flush data to disk and have it comparable
		for _, class := range []*models.Class{classDynamic, classFlat, classHnsw} {
			require.NoError(t,
				c.Schema().TenantsUpdater().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx),
			)
			require.NoError(t,
				c.Schema().TenantsUpdater().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx),
			)
		}

		colHNSW, err := getDebugUsageWithPortAndCollection(debug, classNameHnsw)
		require.NoError(t, err)
		require.NotNil(t, colHNSW)
		require.Len(t, colHNSW.Shards, 1)
		shardHNSW := colHNSW.Shards[0]
		require.Equal(t, objectCount2+10, *shardHNSW.ObjectsCount)
		require.Len(t, shardHNSW.NamedVectors, 1)
		require.NotNil(t, shardHNSW.ObjectsStorageBytes)
		require.NotNil(t, shardHNSW.VectorStorageBytes)
		vectorHNSW := shardHNSW.NamedVectors[0]

		colDynamicHNSW, err := getDebugUsageWithPortAndCollection(debug, classNameDynamic)
		require.NoError(t, err)
		require.NotNil(t, colDynamicHNSW)
		shardDynamicHNSW := colDynamicHNSW.Shards[0]
		require.Equal(t, objectCount2+10, *shardDynamicHNSW.ObjectsCount)
		require.Len(t, shardDynamicHNSW.NamedVectors, 1)
		require.NotNil(t, shardDynamicHNSW.ObjectsStorageBytes)
		require.NotNil(t, shardDynamicHNSW.VectorStorageBytes)
		vectorDynamicHNSW := shardDynamicHNSW.NamedVectors[0]

		// there might be some small differences in the object storage due to class
		require.InDelta(t, *shardDynamicHNSW.ObjectsStorageBytes, *shardHNSW.ObjectsStorageBytes, float64(*shardDynamicHNSW.ObjectsStorageBytes)*0.1)
		require.Equal(t, *shardDynamicHNSW.VectorStorageBytes, *shardHNSW.VectorStorageBytes)
		require.Equal(t, vectorDynamicHNSW.Dimensionalities, vectorHNSW.Dimensionalities)
	})
}

func sanitizeName(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	return name
}
