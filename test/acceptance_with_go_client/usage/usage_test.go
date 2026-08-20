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
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"acceptance_tests_with_client/internal/wvhost"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
)

func TestTenantStatusChanges(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if endUsage.Load() {
				return
			}

			usage, err := GetDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			require.Equal(t, len(usage.Shards), len(tenants))

			require.Equal(t, len(usage.Shards), usage.UniqueShardCount)

			names := make(map[string]struct{})
			for _, shard := range usage.Shards {
				if _, ok := names[shard.Name]; ok {
					require.Fail(t, "duplicate shard name found")
				}
				names[shard.Name] = struct{}{}
			}
			require.Equal(t, len(names), len(tenants))
		}
	}()
	defer func() {
		endUsage.Store(true)
		wg.Wait()
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
}

func TestUsageTenantDelete(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if endUsage.Load() {
				return
			}
			deletedTenantsBeforeCall := deletedTenants.Load()
			usage, err := GetDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			deletedTenantsAfterCall := deletedTenants.Load()

			// we add a bit of wiggle room here as the usage endpoint might take a bit to reflect the changes
			require.LessOrEqual(t, len(usage.Shards), len(tenants)-int(deletedTenantsBeforeCall)+1)
			require.GreaterOrEqual(t, len(usage.Shards), len(tenants)-int(deletedTenantsAfterCall)-1)

			if len(usage.Shards) > 0 {
				require.Equal(t, len(usage.Shards), usage.UniqueShardCount)
			}

			names := make(map[string]struct{})
			for _, shard := range usage.Shards {
				if _, ok := names[shard.Name]; ok {
					require.Fail(t, "duplicate shard name found")
				}
				names[shard.Name] = struct{}{}
			}
		}
	}()

	defer func() {
		endUsage.Store(true)
		wg.Wait()
	}()

	for i := range tenants {
		err := c.Schema().TenantsDeleter().WithClassName(className).WithTenants(tenants[i].Name).Do(ctx)
		require.NoError(t, err)
		deletedTenants.Add(1)
	}
}

func TestCollectionDeletion(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	defer func() {
		endUsage.Store(true)
		wg.Wait()
	}()

	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)
		require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))
		deletedClasses.Add(1)
	}
}

func TestAlterSchemaDropPropertyIndex(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.NoError(t, err)

	className := t.Name() + "Class"
	textProp := "title"
	numberProp := "count"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:            textProp,
				DataType:        []string{schema.DataTypeText.String()},
				IndexFilterable: new(true),
				IndexSearchable: new(true),
			},
			{
				Name:              numberProp,
				DataType:          []string{schema.DataTypeNumber.String()},
				IndexFilterable:   new(true),
				IndexRangeFilters: new(true),
			},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	// Insert 100 objects
	const numObjects = 100
	objs := make([]*models.Object, numObjects)
	for i := range numObjects {
		objs[i] = &models.Object{
			Class: className,
			ID:    strfmt.UUID(uuid.NewString()),
			Properties: map[string]any{
				textProp:   fmt.Sprintf("title number %d", i),
				numberProp: float64(i),
			},
		}
	}
	batchResp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
	require.NoError(t, err)
	for _, r := range batchResp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		require.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *r.Result.Status)
	}

	// Record initial shard storage
	colUsageBefore, err := GetDebugUsageForCollection(className)
	require.NoError(t, err)
	require.Len(t, colUsageBefore.Shards, 1)
	initialStorage := colUsageBefore.Shards[0].FullShardStorageBytes
	require.Greater(t, initialStorage, uint64(0))

	// Concurrently poll shard usage while dropping property indices
	endUsage := atomic.Bool{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if endUsage.Load() {
				return
			}
			time.Sleep(500 * time.Millisecond)
			if endUsage.Load() {
				return
			}
			usage, err := GetDebugUsageForCollection(className)
			require.NoError(t, err)
			require.Len(t, usage.Shards, 1)
			require.Greater(t, usage.Shards[0].FullShardStorageBytes, uint64(0))
			assert.Less(t, usage.Shards[0].FullShardStorageBytes, initialStorage)
		}
	}()

	// Drop all property indices using the Go client
	require.NoError(t, c.Schema().PropertyIndexDeleter().
		WithClassName(className).WithPropertyName(textProp).WithFilterable().Do(ctx))
	require.NoError(t, c.Schema().PropertyIndexDeleter().
		WithClassName(className).WithPropertyName(textProp).WithSearchable().Do(ctx))
	require.NoError(t, c.Schema().PropertyIndexDeleter().
		WithClassName(className).WithPropertyName(numberProp).WithFilterable().Do(ctx))
	require.NoError(t, c.Schema().PropertyIndexDeleter().
		WithClassName(className).WithPropertyName(numberProp).WithRangeFilters().Do(ctx))

	endUsage.Store(true)
	wg.Wait()

	// Verify that storage dropped after removing all property indices
	colUsageAfter, err := GetDebugUsageForCollection(className)
	require.NoError(t, err)
	require.Len(t, colUsageAfter.Shards, 1)
	assert.Less(t, colUsageAfter.Shards[0].FullShardStorageBytes, initialStorage)
}

func TestAlterSchemaDropVectorIndex(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: wvhost.REST()})
	require.NoError(t, err)

	className := t.Name() + "Class"
	tenantName := "tenant"
	// "vector" is a proper prefix of the other two names, so at 384 dimensions its key sorts
	// after theirs in the dimensions bucket
	vectorPrefix := "vector"
	vector1 := "vector1"
	vector2 := "vector2"

	// every vector gets its own dimensions and object count, so a report that attributes one
	// vector's numbers to another cannot pass
	expected := map[string]usagetypes.Dimensionality{
		vectorPrefix: {Dimensions: 384, Count: 100},
		vector1:      {Dimensions: 128, Count: 60},
		vector2:      {Dimensions: 256, Count: 30},
	}

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name:     "description",
				DataType: []string{schema.DataTypeText.String()},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			vectorPrefix: {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
			vector1: {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "hnsw",
			},
			vector2: {
				Vectorizer: map[string]any{
					"none": map[string]any{},
				},
				VectorIndexType: "flat",
			},
		},
		// enables the COLD/HOT flush below for a stable on-disk baseline
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName}).Do(ctx))

	// Insert 100 objects, each carrying the named vectors its index still falls within
	const numObjects = 100
	objs := make([]*models.Object, numObjects)
	for i := range numObjects {
		vectors := models.Vectors{}
		for name, dimensionality := range expected {
			if i < dimensionality.Count {
				vectors[name] = generateRandomVector(dimensionality.Dimensions)
			}
		}
		objs[i] = &models.Object{
			Class:  className,
			ID:     strfmt.UUID(uuid.NewString()),
			Tenant: tenantName,
			Properties: map[string]any{
				"name":        fmt.Sprintf("name %d", i),
				"description": fmt.Sprintf("description %d", i),
			},
			Vectors: vectors,
		}
	}
	batchResp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
	require.NoError(t, err)
	for _, r := range batchResp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		require.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *r.Result.Status)
	}

	testAllObjectsIndexed(t, c, className)

	// COLD/HOT cycle flushes to disk so the baseline is comparable post-drop
	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

	// the cold shard is reported straight from the dimensions bucket on disk
	colUsageCold, err := GetDebugUsageForCollection(className)
	require.NoError(t, err)
	require.Len(t, colUsageCold.Shards, 1)
	require.Equal(t, expected, namedVectorDimensionalities(t, colUsageCold.Shards[0]))

	// Drop vector2's index while the tenant stays cold. The report above saved its
	// numbers to the shard directory and only loading the shard deletes that file,
	// so the next report has to notice the schema changed under it.
	require.NoError(t, c.Schema().VectorIndexDeleter().
		WithClassName(className).WithVectorIndexName(vector2).Do(ctx))
	delete(expected, vector2)
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		colUsageColdAfterDrop, err := GetDebugUsageForCollection(className)
		require.NoError(ct, err)
		require.Len(ct, colUsageColdAfterDrop.Shards, 1)
		require.Equal(ct, expected, namedVectorDimensionalities(ct, colUsageColdAfterDrop.Shards[0]))
	}, 30*time.Second, 500*time.Millisecond)

	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx))

	// Verify all named vectors appear in usage. The baseline waits for the
	// reactivated shard to stop growing: reloading it makes every HNSW index
	// snapshot its commit log, and one snapshot is a whole 4 MiB block whatever it
	// holds. Sampling before those land measures a shard that is still only on
	// disk against one that has reloaded, and the drop below then reads as growth.
	shard := settledShardUsage(t, c, className, tenantName)
	require.Equal(t, int64(numObjects), shard.ObjectsCount)
	require.Equal(t, expected, namedVectorDimensionalities(t, shard))

	initialFullStorage := shard.FullShardStorageBytes
	initialVectorStorage := shard.VectorStorageBytes
	require.Greater(t, initialFullStorage, uint64(0))
	require.Greater(t, initialVectorStorage, uint64(0))

	// Drop vector1's index
	require.NoError(t, c.Schema().VectorIndexDeleter().
		WithClassName(className).WithVectorIndexName(vector1).Do(ctx))

	// Verify vector1 is no longer in the usage metrics, the others remain,
	// and storage bytes have decreased
	delete(expected, vector1)
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		colUsageAfter, err := GetDebugUsageForCollection(className)
		require.NoError(ct, err)
		require.Len(ct, colUsageAfter.Shards, 1)
		shardAfter := colUsageAfter.Shards[0]
		require.Equal(ct, int64(numObjects), shardAfter.ObjectsCount)
		require.Equal(ct, expected, namedVectorDimensionalities(ct, shardAfter))
		assert.Less(ct, shardAfter.FullShardStorageBytes, initialFullStorage)
		assert.Less(ct, shardAfter.VectorStorageBytes, initialVectorStorage)
	}, 30*time.Second, 500*time.Millisecond)
}

func TestRestart(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithDebugPort().
		WithWeaviateEnv("TRACK_VECTOR_DIMENSIONS", "true").
		WithWeaviateEnv(entcfg.EnvNestedFilteringPreview, "true").
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
			{
				Name:     "cars",
				DataType: []string{string(schema.DataTypeObjectArray)},
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "make",
						DataType:        []string{string(schema.DataTypeText)},
						Tokenization:    models.NestedPropertyTokenizationField,
						IndexFilterable: new(true),
					},
					{
						Name:            "model",
						DataType:        []string{string(schema.DataTypeText)},
						Tokenization:    models.NestedPropertyTokenizationField,
						IndexFilterable: new(true),
					},
				},
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

	// add some data. Only even tenants get nested values, leaving the odd ones as
	// a baseline with the same schema and object count.
	for i, tenant := range tenants {
		objs := make([]*models.Object, 10)
		for j := range objs {
			vector := make([]float32, 128)
			for k := range vector {
				vector[k] = float32(i+j+k) / 10000.0
			}
			props := map[string]any{
				"first": fmt.Sprintf("hello%d-%d", i, j),
			}
			if i%2 == 0 {
				props["cars"] = nestedCars(i, j)
			}
			objs[j] = &models.Object{
				Class:      className,
				Properties: props,
				Vector:     vector,
				Tenant:     tenant.Name,
			}
		}

		batchResp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
		require.NoError(t, err)
		for _, r := range batchResp {
			require.NotNil(t, r.Result)
			require.NotNil(t, r.Result.Status)
			require.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *r.Result.Status)
		}
	}

	loaded := strings.ToLower(models.TenantActivityStatusACTIVE)
	fromDisk := strings.ToLower(models.TenantActivityStatusINACTIVE)

	// collect with concurrent shard readers, compare with the default report after restart
	usage, err := getDebugUsageWithPort(debug, 4)
	require.NoError(t, err)
	require.NotNil(t, usage)
	assertNestedIndexStorageCounted(t, usage, className, loaded)

	require.NoError(t, compose.Stop(ctx, compose.GetWeaviate().Name(), nil))

	err = compose.Start(ctx, compose.GetWeaviate().Name())
	require.NoError(t, err)

	usage2, err := getDebugUsageWithPort(compose.GetWeaviate().DebugURI())
	require.NoError(t, err)
	require.NotNil(t, usage2)
	require.NoError(t, ReportsDifference(usage, usage2))
	assertNestedIndexStorageCounted(t, usage2, className, loaded)

	// cold tenants are measured from the bucket directories instead of the shard
	cold := make([]models.Tenant, len(tenants))
	for i := range tenants {
		cold[i] = models.Tenant{Name: tenants[i].Name, ActivityStatus: models.TenantActivityStatusCOLD}
	}
	// the restart gave the container a new mapped port
	c, err = client.NewClient(client.Config{Scheme: "http", Host: compose.GetWeaviate().URI()})
	require.NoError(t, err)
	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).WithTenants(cold...).Do(ctx))

	usage3, err := getDebugUsageWithPort(compose.GetWeaviate().DebugURI())
	require.NoError(t, err)
	require.NotNil(t, usage3)
	assertNestedIndexStorageCounted(t, usage3, className, fromDisk)

	// shares this container instead of starting its own
	t.Run("muvera usage", func(t *testing.T) {
		testUsageMuvera(t, c, compose.GetWeaviate().DebugURI())
	})
}

// nestedCars keeps every leaf value distinct, so each entry adds its own keys to
// the nested property buckets.
func nestedCars(tenant, object int) []any {
	cars := make([]any, 20)
	for i := range cars {
		cars[i] = map[string]any{
			"make":  fmt.Sprintf("make-%d-%d-%d", tenant, object, i),
			"model": fmt.Sprintf("model-%d-%d-%d", tenant, object, i),
		}
	}
	return cars
}

func collectionShards(t *testing.T, report *usagetypes.Report, className string) usagetypes.ShardsUsage {
	t.Helper()

	for _, col := range report.Collections {
		if col != nil && col.Name == className {
			return col.Shards
		}
	}
	require.FailNow(t, "collection missing from usage report: "+className)
	return nil
}

// assertNestedIndexStorageCounted checks that tenants with nested values report more
// than double the index storage of those without — nested data lives only in the
// property.nested_ / property.nestedmeta_ buckets, so skipping those makes the two
// groups equal. wantStatus is active for a loaded shard, inactive for one from disk.
func assertNestedIndexStorageCounted(t *testing.T, report *usagetypes.Report, className, wantStatus string) {
	t.Helper()

	// per shard rather than summed, so one shard reporting nothing cannot hide
	var smallestNested, largestBaseline uint64
	for _, shard := range collectionShards(t, report, className) {
		require.Equal(t, wantStatus, shard.Status, "shard %s", shard.Name)
		require.GreaterOrEqual(t, shard.FullShardStorageBytes, shard.IndexStorageBytes,
			"shard %s full storage must contain its index storage", shard.Name)

		tenant, err := strconv.Atoi(strings.TrimPrefix(shard.Name, "tenant"))
		require.NoError(t, err)
		if tenant%2 == 0 {
			if smallestNested == 0 || shard.IndexStorageBytes < smallestNested {
				smallestNested = shard.IndexStorageBytes
			}
		} else if shard.IndexStorageBytes > largestBaseline {
			largestBaseline = shard.IndexStorageBytes
		}
	}

	require.Greater(t, largestBaseline, uint64(0), "baseline tenants should report index storage")
	require.Greater(t, smallestNested, 2*largestBaseline,
		"every tenant with nested values must report the nested property buckets on top of the baseline")
}

// testAllObjectsIndexed waits for every shard of a class to finish indexing. A
// class with several named vectors fills one queue per vector, and a quantizer
// trains on top of that, so the wait is far longer than a single queue needs.
func testAllObjectsIndexed(t *testing.T, c *client.Client, className string) {
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
	}, 2*time.Minute, 500*time.Millisecond)
}

// namedVectors maps every named vector of a shard usage report to its usage
// entry, requiring every wanted name to be among them.
func namedVectors(t require.TestingT, shard *usagetypes.ShardUsage, want ...string) map[string]*usagetypes.VectorUsage {
	vectors := make(map[string]*usagetypes.VectorUsage, len(shard.NamedVectors))
	for _, v := range shard.NamedVectors {
		vectors[v.Name] = v
	}
	for _, name := range want {
		require.Contains(t, vectors, name)
	}
	return vectors
}

// settledShardUsage loads a tenant's shard and reports its usage once the shard has
// stopped growing, so a caller's baseline is measured in the state the reports it is
// compared against are measured in. A reload lets the HNSW commit-log maintenance
// cycle run, which writes files a shard sitting on disk does not have; the cycle
// backs off to 10s between runs, so the shard has to hold still for longer than that
// before it counts as settled.
func settledShardUsage(t *testing.T, c *client.Client, className, tenantName string) *usagetypes.ShardUsage {
	t.Helper()

	// reading the tenant materializes its lazily loaded shard before the wait starts
	_, err := c.Data().ObjectsGetter().WithClassName(className).
		WithTenant(tenantName).WithLimit(1).Do(context.Background())
	require.NoError(t, err)

	const settleFor = 15 * time.Second
	var settled *usagetypes.ShardUsage
	var unchangedSince time.Time
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		colUsage, err := GetDebugUsageForCollection(className)
		require.NoError(ct, err)
		require.Len(ct, colUsage.Shards, 1)
		current := colUsage.Shards[0]

		if settled == nil || current.FullShardStorageBytes != settled.FullShardStorageBytes ||
			current.VectorStorageBytes != settled.VectorStorageBytes {
			settled, unchangedSince = current, time.Now()
		}
		if held := time.Since(unchangedSince); held < settleFor {
			ct.Errorf("shard %q still growing: %d full / %d vector bytes, unchanged for %s",
				current.Name, current.FullShardStorageBytes, current.VectorStorageBytes, held)
		}
	}, 90*time.Second, 500*time.Millisecond)

	return settled
}

// namedVectorDimensionalities maps every named vector of a shard usage report to the
// dimensionality it reports.
func namedVectorDimensionalities(t require.TestingT, shard *usagetypes.ShardUsage) map[string]usagetypes.Dimensionality {
	dimensionalities := make(map[string]usagetypes.Dimensionality, len(shard.NamedVectors))
	for name, v := range namedVectors(t, shard) {
		require.NotEmpty(t, v.Dimensionalities, "no dimensionality reported for vector %q", name)
		dimensionalities[name] = *v.Dimensionalities[0]
	}
	return dimensionalities
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
	hnswBQ := "hnswbq"
	hnswPQ := "hnswpq"
	hnswRQ := "hnswrq"
	hnswSQ := "hnswsq"
	flatBQ := "flatbq"
	flatRQ := "flatrq"
	dimensions := 1024
	// the quantizer vectors below only have to prove the ratio each quantizer
	// reports, and no quantizer derives that from the width, so they stay narrow
	// to keep their shared tenant cheap to insert and index
	quantizerDimensions := 128
	flat := "flat"
	bq := "bq"
	hnsw := "hnsw"
	pq := "pq"

	// bq stores one bit per dimension instead of 32
	bqCompressionRatio := float64(32)
	// sq stores one byte per dimension instead of four
	sqCompressionRatio := float64(4)
	// rq on 8 bits stores one byte per dimension, plus 16 bytes of metadata
	rq8CompressionRatio := float64(dimensions*4) / float64(16+dimensions)
	rq8QuantizerRatio := float64(quantizerDimensions*4) / float64(16+quantizerDimensions)
	// hfresh always quantizes with rq on 1 bit: one packed byte per 8
	// dimensions, plus 8 bytes of metadata
	rq1CompressionRatio := float64(dimensions*4) / float64(8+dimensions/8)

	objectCount1 := 1000
	objectCount2 := 2000

	targetVectorDimensions := map[string]int{
		dynamic1024: dimensions,
		hnswBQ:      quantizerDimensions,
		hnswPQ:      quantizerDimensions,
		hnswRQ:      quantizerDimensions,
		hnswSQ:      quantizerDimensions,
		flatBQ:      quantizerDimensions,
		flatRQ:      quantizerDimensions,
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
		require.Equal(t, int64(objectCount1), shard.ObjectsCount)
		require.Len(t, shard.NamedVectors, 1)
		require.Equal(t, dynamic1024, shard.NamedVectors[0].Name)
		require.Equal(t, flat, shard.NamedVectors[0].VectorIndexType)
		require.Equal(t, bq, shard.NamedVectors[0].Compression)
		require.True(t, shard.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shard.NamedVectors[0].Dimensionalities)
		require.Equal(t, dimensions, shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, shard.NamedVectors[0].Dimensionalities[0].Count)

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
			require.Equal(ct, int64(objectCount2), shardHnsw.ObjectsCount)
			require.Len(ct, shardHnsw.NamedVectors, 1)
			require.Equal(ct, dynamic1024, shardHnsw.NamedVectors[0].Name)
			require.Equal(ct, hnsw, shardHnsw.NamedVectors[0].VectorIndexType)
			require.True(ct, shardHnsw.NamedVectors[0].IsDynamic)
			require.Equal(ct, pq, shardHnsw.NamedVectors[0].Compression)
			require.NotEmpty(ct, shardHnsw.NamedVectors[0].Dimensionalities)
			require.Equal(ct, dimensions, shardHnsw.NamedVectors[0].Dimensionalities[0].Dimensions)
			require.Equal(ct, objectCount2, shardHnsw.NamedVectors[0].Dimensionalities[0].Count)
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
				hnswBQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: hnsw,
					VectorIndexConfig: map[string]any{
						bq: map[string]any{
							"enabled": true,
						},
					},
				},
				hnswRQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: hnsw,
					VectorIndexConfig: map[string]any{
						"rq": map[string]any{
							"enabled": true,
						},
					},
				},
				// sq trains at 100k objects by default, which this tenant would
				// never reach; the low limit lets it compress while indexing
				hnswSQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: hnsw,
					VectorIndexConfig: map[string]any{
						"sq": map[string]any{
							"enabled":       true,
							"trainingLimit": float64(100),
						},
					},
				},
				// pq trains at 100k objects, far above what this tenant holds,
				// so its vectors stay uncompressed on disk
				hnswPQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: hnsw,
					VectorIndexConfig: map[string]any{
						pq: map[string]any{
							"enabled": true,
						},
					},
				},
				// a flat index reaches its quantizer through neither the hnsw
				// nor the dynamic config, so it has to be covered on its own
				flatBQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: flat,
					VectorIndexConfig: map[string]any{
						bq: map[string]any{
							"enabled": true,
						},
					},
				},
				flatRQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: flat,
					VectorIndexConfig: map[string]any{
						"rq": map[string]any{
							"enabled": true,
						},
					},
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}

		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		tenantName := "tenant"
		c.Schema().TenantsCreator().WithClassName(className).WithTenants(models.Tenant{Name: "tenant"}).Do(ctx)

		insertObjects(t, 1000, c, className, tenantName, models.Vectors{
			dynamic1024: generateRandomVector(targetVectorDimensions[dynamic1024]),
			hnswBQ:      generateRandomVector(targetVectorDimensions[hnswBQ]),
			hnswPQ:      generateRandomVector(targetVectorDimensions[hnswPQ]),
			hnswRQ:      generateRandomVector(targetVectorDimensions[hnswRQ]),
			hnswSQ:      generateRandomVector(targetVectorDimensions[hnswSQ]),
			flatBQ:      generateRandomVector(targetVectorDimensions[flatBQ]),
			flatRQ:      generateRandomVector(targetVectorDimensions[flatRQ]),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		// sq trains off the indexing queue, so the ratio it reports lags the
		// objects being indexed
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			colUsage, err := getDebugUsageWithPortAndCollection(debug, className)
			require.NoError(ct, err)
			require.Len(ct, colUsage.Shards, 1)
			sq := namedVectors(ct, colUsage.Shards[0], hnswSQ)[hnswSQ]
			require.Equal(ct, sqCompressionRatio, sq.VectorCompressionRatio)
		}, 2*time.Minute, 500*time.Millisecond)

		colHot, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colHot)

		require.Len(t, colHot.Shards, 1)
		shardHot := colHot.Shards[0]
		require.Equal(t, int64(objectCount1), shardHot.ObjectsCount)
		require.Len(t, shardHot.NamedVectors, 7)
		hotVectors := namedVectors(t, shardHot, dynamic1024, hnswBQ, hnswPQ, hnswRQ, hnswSQ, flatBQ, flatRQ)
		require.Equal(t, flat, hotVectors[dynamic1024].VectorIndexType)
		require.Equal(t, bq, hotVectors[dynamic1024].Compression)
		require.True(t, hotVectors[dynamic1024].IsDynamic)
		require.Equal(t, bqCompressionRatio, hotVectors[dynamic1024].VectorCompressionRatio)
		require.NotEmpty(t, hotVectors[dynamic1024].Dimensionalities)
		require.Equal(t, dimensions, hotVectors[dynamic1024].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, hotVectors[dynamic1024].Dimensionalities[0].Count)
		require.Equal(t, hnsw, hotVectors[hnswBQ].VectorIndexType)
		require.Equal(t, bq, hotVectors[hnswBQ].Compression)
		require.False(t, hotVectors[hnswBQ].IsDynamic)
		require.Equal(t, bqCompressionRatio, hotVectors[hnswBQ].VectorCompressionRatio)
		require.Equal(t, pq, hotVectors[hnswPQ].Compression)
		require.Equal(t, float64(1), hotVectors[hnswPQ].VectorCompressionRatio,
			"pq compresses nothing before it trains")
		require.Equal(t, "rq", hotVectors[hnswRQ].Compression)
		require.Equal(t, int16(8), hotVectors[hnswRQ].Bits)
		require.InDelta(t, rq8QuantizerRatio, hotVectors[hnswRQ].VectorCompressionRatio, 0.001)
		require.Equal(t, "sq", hotVectors[hnswSQ].Compression)
		require.Equal(t, sqCompressionRatio, hotVectors[hnswSQ].VectorCompressionRatio)
		require.Equal(t, flat, hotVectors[flatBQ].VectorIndexType)
		require.Equal(t, bq, hotVectors[flatBQ].Compression)
		require.False(t, hotVectors[flatBQ].IsDynamic)
		require.Equal(t, bqCompressionRatio, hotVectors[flatBQ].VectorCompressionRatio)
		require.Equal(t, flat, hotVectors[flatRQ].VectorIndexType)
		require.Equal(t, "rq", hotVectors[flatRQ].Compression)
		require.Equal(t, int16(8), hotVectors[flatRQ].Bits)
		require.InDelta(t, rq8QuantizerRatio, hotVectors[flatRQ].VectorCompressionRatio, 0.001)

		require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

		colCold, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colCold)

		require.Len(t, colCold.Shards, 1)
		shardCold := colCold.Shards[0]
		require.Len(t, shardCold.NamedVectors, 7)
		coldVectors := namedVectors(t, shardCold, dynamic1024, hnswBQ, hnswPQ, hnswRQ, hnswSQ, flatBQ, flatRQ)
		// a cold tenant reports what it reported while hot: the dynamic index
		// is read as the flat one it has not upgraded away from, and both
		// compression ratios follow from the config plus the quantized vectors
		// the shard actually holds
		require.Equal(t, flat, coldVectors[dynamic1024].VectorIndexType)
		require.Equal(t, bq, coldVectors[dynamic1024].Compression)
		require.True(t, coldVectors[dynamic1024].IsDynamic)
		require.Equal(t, bqCompressionRatio, coldVectors[dynamic1024].VectorCompressionRatio)
		require.NotEmpty(t, coldVectors[dynamic1024].Dimensionalities)
		require.Equal(t, dimensions, coldVectors[dynamic1024].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, coldVectors[dynamic1024].Dimensionalities[0].Count)
		require.Equal(t, hnsw, coldVectors[hnswBQ].VectorIndexType)
		require.Equal(t, bq, coldVectors[hnswBQ].Compression)
		require.False(t, coldVectors[hnswBQ].IsDynamic)
		require.Equal(t, bqCompressionRatio, coldVectors[hnswBQ].VectorCompressionRatio)
		require.Equal(t, pq, coldVectors[hnswPQ].Compression)
		require.Equal(t, float64(1), coldVectors[hnswPQ].VectorCompressionRatio,
			"the config asks for pq, but no quantized vectors exist to bill for")
		require.Equal(t, "rq", coldVectors[hnswRQ].Compression)
		require.Equal(t, int16(8), coldVectors[hnswRQ].Bits)
		require.InDelta(t, rq8QuantizerRatio, coldVectors[hnswRQ].VectorCompressionRatio, 0.001)
		// sq trained while hot, so its quantized vectors are on disk to bill for
		require.Equal(t, "sq", coldVectors[hnswSQ].Compression)
		require.Equal(t, sqCompressionRatio, coldVectors[hnswSQ].VectorCompressionRatio)
		require.Equal(t, flat, coldVectors[flatBQ].VectorIndexType)
		require.Equal(t, bq, coldVectors[flatBQ].Compression)
		require.False(t, coldVectors[flatBQ].IsDynamic)
		require.Equal(t, bqCompressionRatio, coldVectors[flatBQ].VectorCompressionRatio)
		require.Equal(t, flat, coldVectors[flatRQ].VectorIndexType)
		require.Equal(t, "rq", coldVectors[flatRQ].Compression)
		require.Equal(t, int16(8), coldVectors[flatRQ].Bits)
		require.InDelta(t, rq8QuantizerRatio, coldVectors[flatRQ].VectorCompressionRatio, 0.001)

		// the first cold report saves itself to disk, and every later one is
		// served from that file rather than recomputed
		colCached, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NoError(t, CollectionUsageDifference(colCold, colCached))
	})

	// A tenant that crossed the dynamic threshold before going cold: the report
	// has to read the upgrade from disk, or it bills the flat side of the config
	// for a tenant whose vectors hnsw now holds.
	t.Run("multi tenant upgraded to hnsw", func(t *testing.T) {
		className := sanitizeName("Class" + t.Name())
		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		// pq trains one centroid per cluster and needs at least as many vectors
		// as it has centroids, 256 by default. The shared config trains on 100,
		// too few to fit, which leaves its index uncompressed.
		upgradingVectorIndexConfig := map[string]any{
			"threshold": 1001,
			hnsw: map[string]any{
				pq: map[string]any{
					"enabled":       true,
					"trainingLimit": float64(512),
				},
			},
			flat: map[string]any{
				bq: map[string]any{
					"enabled": true,
				},
			},
		}

		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				dynamic1024: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType:   "dynamic",
					VectorIndexConfig: upgradingVectorIndexConfig,
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		tenantName := "tenant"
		require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).
			WithTenants(models.Tenant{Name: tenantName}).Do(ctx))

		// past the threshold of 1001, so the index upgrades to hnsw and trains pq
		insertObjects(t, objectCount2, c, className, tenantName, models.Vectors{
			dynamic1024: generateRandomVector(targetVectorDimensions[dynamic1024]),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		var hotRatio float64
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			colHot, err := getDebugUsageWithPortAndCollection(debug, className)
			require.NoError(ct, err)
			require.Len(ct, colHot.Shards, 1)
			hot := namedVectors(ct, colHot.Shards[0], dynamic1024)[dynamic1024]
			require.Equal(ct, hnsw, hot.VectorIndexType)
			require.Equal(ct, pq, hot.Compression)
			// Compression comes from the config, so only a ratio above 1 says pq
			// finished training and wrote the quantized vectors a cold read bills.
			require.Greater(ct, hot.VectorCompressionRatio, float64(1))
			hotRatio = hot.VectorCompressionRatio
		}, 5*time.Minute, 500*time.Millisecond)

		require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
			WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

		colCold, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.Len(t, colCold.Shards, 1)
		cold := namedVectors(t, colCold.Shards[0], dynamic1024)[dynamic1024]
		require.Equal(t, hnsw, cold.VectorIndexType, "reading flat here would bill the wrong side of the config")
		require.Equal(t, pq, cold.Compression)
		require.True(t, cold.IsDynamic)
		// deactivating a tenant must not change the ratio it bills
		require.Equal(t, hotRatio, cold.VectorCompressionRatio)
	})

	// hfresh quantizes with rq on 1 bit whatever its config says, so a cold
	// tenant has to report that ratio without reading its index.
	t.Run("multi tenant hfresh", func(t *testing.T) {
		className := sanitizeName("Class" + t.Name())
		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		hfreshVector := "hfreshrq"
		class := &models.Class{
			Class: className,
			VectorConfig: map[string]models.VectorConfig{
				hfreshVector: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "hfresh",
				},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		tenantName := "tenant"
		require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).
			WithTenants(models.Tenant{Name: tenantName}).Do(ctx))

		insertObjects(t, objectCount1, c, className, tenantName, models.Vectors{
			hfreshVector: generateRandomVector(dimensions),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		colHot, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.Len(t, colHot.Shards, 1)
		hot := namedVectors(t, colHot.Shards[0], hfreshVector)[hfreshVector]
		require.Equal(t, "auto", hot.Compression)
		require.InDelta(t, rq1CompressionRatio, hot.VectorCompressionRatio, 0.001)

		require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
			WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))

		colCold, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.Len(t, colCold.Shards, 1)
		cold := namedVectors(t, colCold.Shards[0], hfreshVector)[hfreshVector]
		require.Equal(t, "auto", cold.Compression)
		require.InDelta(t, rq1CompressionRatio, cold.VectorCompressionRatio, 0.001)
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
		require.Equal(t, int64(objectCount1), shard.ObjectsCount)
		require.Len(t, shard.NamedVectors, 1)
		require.Equal(t, flat, shard.NamedVectors[0].VectorIndexType)
		require.Equal(t, bq, shard.NamedVectors[0].Compression)
		require.True(t, shard.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shard.NamedVectors[0].Dimensionalities)
		require.Equal(t, dimensions, shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, shard.NamedVectors[0].Dimensionalities[0].Count)
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
		require.Equal(t, int64(objectCount1), shardFlat.ObjectsCount)
		require.Len(t, shardFlat.NamedVectors, 1)
		vectorFlat := shardFlat.NamedVectors[0]

		colDynamic, err := getDebugUsageWithPortAndCollection(debug, classNameDynamic)
		require.NoError(t, err)
		require.NotNil(t, colDynamic)
		shardDynamic := colDynamic.Shards[0]
		require.Equal(t, int64(objectCount1), shardDynamic.ObjectsCount)
		require.Len(t, shardDynamic.NamedVectors, 1)
		vectorDynamic := shardDynamic.NamedVectors[0]

		require.InDelta(t, shardDynamic.ObjectsStorageBytes, shardFlat.ObjectsStorageBytes, float64(shardDynamic.ObjectsStorageBytes)*0.05)
		require.Equal(t, shardDynamic.VectorStorageBytes, shardFlat.VectorStorageBytes)
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
			require.Equal(ct, dynamic1024, shardHnsw.NamedVectors[0].Name)
			require.Equal(ct, hnsw, shardHnsw.NamedVectors[0].VectorIndexType)
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
		require.Equal(t, int64(objectCount2+10), shardHNSW.ObjectsCount)
		require.Len(t, shardHNSW.NamedVectors, 1)
		vectorHNSW := shardHNSW.NamedVectors[0]

		colDynamicHNSW, err := getDebugUsageWithPortAndCollection(debug, classNameDynamic)
		require.NoError(t, err)
		require.NotNil(t, colDynamicHNSW)
		shardDynamicHNSW := colDynamicHNSW.Shards[0]
		require.Equal(t, int64(objectCount2+10), shardDynamicHNSW.ObjectsCount)
		require.Len(t, shardDynamicHNSW.NamedVectors, 1)
		vectorDynamicHNSW := shardDynamicHNSW.NamedVectors[0]

		// there might be some small differences in the object storage due to class
		require.InDelta(t, shardDynamicHNSW.ObjectsStorageBytes, shardHNSW.ObjectsStorageBytes, float64(shardDynamicHNSW.ObjectsStorageBytes)*0.1)
		require.Equal(t, vectorDynamicHNSW.Dimensionalities, vectorHNSW.Dimensionalities)
	})

	t.Run("dynamic with RQ", func(t *testing.T) {
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
					VectorIndexType: "dynamic",
					VectorIndexConfig: map[string]any{
						"threshold": 1001,
						hnsw: map[string]any{
							"rq": map[string]any{
								"enabled": true,
							},
						},
						flat: map[string]any{},
					},
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
		require.Equal(t, int64(objectCount1), shard.ObjectsCount)
		require.Len(t, shard.NamedVectors, 1)
		require.Equal(t, dynamic1024, shard.NamedVectors[0].Name)
		require.Equal(t, flat, shard.NamedVectors[0].VectorIndexType)
		require.Equal(t, "standard", shard.NamedVectors[0].Compression)
		require.True(t, shard.NamedVectors[0].IsDynamic)
		require.NotEmpty(t, shard.NamedVectors[0].Dimensionalities)
		require.Equal(t, dimensions, shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, shard.NamedVectors[0].Dimensionalities[0].Count)

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
			require.Equal(ct, int64(objectCount2), shardHnsw.ObjectsCount)
			require.Len(ct, shardHnsw.NamedVectors, 1)
			require.Equal(ct, dynamic1024, shardHnsw.NamedVectors[0].Name)
			require.Equal(ct, hnsw, shardHnsw.NamedVectors[0].VectorIndexType)
			require.True(ct, shardHnsw.NamedVectors[0].IsDynamic)
			require.Equal(ct, "rq", shardHnsw.NamedVectors[0].Compression)
			require.NotNil(ct, shardHnsw.NamedVectors[0].Bits)
			require.Equal(ct, int16(8), shardHnsw.NamedVectors[0].Bits)
			require.NotEmpty(ct, shardHnsw.NamedVectors[0].Dimensionalities)
			require.Equal(ct, dimensions, shardHnsw.NamedVectors[0].Dimensionalities[0].Dimensions)
			require.Equal(ct, objectCount2, shardHnsw.NamedVectors[0].Dimensionalities[0].Count)
		}, 5*time.Minute, 500*time.Millisecond)
	})

	t.Run("flat with RQ", func(t *testing.T) {
		className := sanitizeName("Class" + t.Name())

		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		singleFlatRQ := "flat_rq"
		targetVectorDimensions := map[string]int{
			singleFlatRQ: 1024,
		}

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
				singleFlatRQ: {
					Vectorizer: map[string]any{
						"none": map[string]any{},
					},
					VectorIndexType: "flat",
					VectorIndexConfig: map[string]any{
						"rq": map[string]any{
							"enabled": true,
						},
					},
				},
			},
		}

		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

		insertObjects(t, 1000, c, className, "", models.Vectors{
			singleFlatRQ: generateRandomVector(targetVectorDimensions[singleFlatRQ]),
		}, nil)
		testAllObjectsIndexed(t, c, className)

		colUsage, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.NotNil(t, colUsage)

		require.Len(t, colUsage.Shards, 1)
		shard := colUsage.Shards[0]
		require.Equal(t, int64(objectCount1), shard.ObjectsCount)
		require.Len(t, shard.NamedVectors, 1)
		require.Equal(t, singleFlatRQ, shard.NamedVectors[0].Name)
		require.Equal(t, flat, shard.NamedVectors[0].VectorIndexType)
		require.Equal(t, "rq", shard.NamedVectors[0].Compression)
		require.NotNil(t, shard.NamedVectors[0].Bits)
		require.Equal(t, int16(8), shard.NamedVectors[0].Bits)
		require.InDelta(t, rq8CompressionRatio, shard.NamedVectors[0].VectorCompressionRatio, 0.001)
		require.NotEmpty(t, shard.NamedVectors[0].Dimensionalities)
		require.Equal(t, dimensions, shard.NamedVectors[0].Dimensionalities[0].Dimensions)
		require.Equal(t, objectCount1, shard.NamedVectors[0].Dimensionalities[0].Count)
	})
}

func sanitizeName(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	return name
}

// testUsageMuvera takes an instance so it can share a testcontainer, it must expose the
// debug port and have TRACK_VECTOR_DIMENSIONS enabled.
func testUsageMuvera(t *testing.T, c *client.Client, debug string) {
	ctx := context.Background()

	className := "UsageMuveraClass"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	tenantName := "tenant"
	muveraVec := "muvera"
	colbertVec := "colbert"
	regularVec := "regular"

	const (
		numObjects  = 20
		tokenDim    = 32
		fixedTokens = 3

		ksim         = 4
		dprojections = 16
		repetitions  = 10
		encodedDims  = repetitions * (1 << ksim) * dprojections
	)

	expected := map[string]usagetypes.Dimensionality{
		muveraVec:  {Dimensions: encodedDims, Count: numObjects},
		colbertVec: {Dimensions: fixedTokens * tokenDim, Count: numObjects},
		regularVec: {Dimensions: tokenDim, Count: numObjects},
	}

	multivectorIndexConfig := func(muvera bool) map[string]any {
		cfg := map[string]any{"enabled": true}
		if muvera {
			cfg["muvera"] = map[string]any{
				"enabled":      true,
				"ksim":         ksim,
				"dprojections": dprojections,
				"repetitions":  repetitions,
			}
		}
		return map[string]any{"multivector": cfg}
	}

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{schema.DataTypeText.String()},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			muveraVec: {
				Vectorizer:        map[string]any{"none": map[string]any{}},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: multivectorIndexConfig(true),
			},
			colbertVec: {
				Vectorizer:        map[string]any{"none": map[string]any{}},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: multivectorIndexConfig(false),
			},
			regularVec: {
				Vectorizer:      map[string]any{"none": map[string]any{}},
				VectorIndexType: "hnsw",
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName}).Do(ctx))

	objs := make([]*models.Object, numObjects)
	for i := range numObjects {
		objs[i] = &models.Object{
			Class:  className,
			ID:     strfmt.UUID(uuid.NewString()),
			Tenant: tenantName,
			Properties: map[string]any{
				"name": fmt.Sprintf("name %d", i),
			},
			Vectors: models.Vectors{
				// varying token counts spread the raw dims over several rows, so a
				// single-row report cannot reach the full count
				muveraVec:  generateRandomMultiVector(2+i%3, tokenDim),
				colbertVec: generateRandomMultiVector(fixedTokens, tokenDim),
				regularVec: generateRandomVector(tokenDim),
			},
		}
	}
	batchResp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
	require.NoError(t, err)
	for _, r := range batchResp {
		require.NotNil(t, r.Result)
		require.NotNil(t, r.Result.Status)
		require.Equal(t, models.ObjectsGetResponseAO2ResultStatusSUCCESS, *r.Result.Status)
	}
	testAllObjectsIndexed(t, c, className)

	// the status pins which path served the report: active = loaded, inactive = read from disk
	assertUsage := func(t require.TestingT, expectedStatus string) {
		colUsage, err := getDebugUsageWithPortAndCollection(debug, className)
		require.NoError(t, err)
		require.Len(t, colUsage.Shards, 1)
		shard := colUsage.Shards[0]
		require.Equal(t, strings.ToLower(expectedStatus), shard.Status)
		if expectedStatus == models.TenantActivityStatusACTIVE {
			require.Equal(t, int64(numObjects), shard.ObjectsCount)
		}
		require.Equal(t, expected, namedVectorDimensionalities(t, shard))

		for _, v := range shard.NamedVectors {
			if v.Name != muveraVec {
				continue
			}
			require.NotNil(t, v.MultiVectorConfig, "muvera vector must report its multi-vector config")
			require.NotNil(t, v.MultiVectorConfig.MuveraConfig)
			assert.True(t, v.MultiVectorConfig.MuveraConfig.Enabled)
			assert.Equal(t, ksim, v.MultiVectorConfig.MuveraConfig.KSim)
			assert.Equal(t, dprojections, v.MultiVectorConfig.MuveraConfig.DProjections)
			assert.Equal(t, repetitions, v.MultiVectorConfig.MuveraConfig.Repetitions)
		}
	}

	// the tenant status flips before the local shard finishes activating, so poll until converged
	assertUsageEventually := func(expectedStatus string) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			assertUsage(ct, expectedStatus)
		}, 30*time.Second, 500*time.Millisecond)
	}

	assertUsageEventually(models.TenantActivityStatusACTIVE)

	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))
	assertUsageEventually(models.TenantActivityStatusINACTIVE)
	assertUsage(t, models.TenantActivityStatusINACTIVE)

	require.NoError(t, c.Schema().TenantsUpdater().WithClassName(className).
		WithTenants(models.Tenant{Name: tenantName, ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx))
	assertUsageEventually(models.TenantActivityStatusACTIVE)
}

func generateRandomMultiVector(tokens, dimensionality int) [][]float32 {
	multiVector := make([][]float32, tokens)
	for i := range multiVector {
		multiVector[i] = generateRandomVector(dimensionality)
	}
	return multiVector
}
