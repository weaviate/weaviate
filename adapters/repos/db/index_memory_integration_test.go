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

//go:build integrationTest

package db

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmemory"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	memoryTestClass = "MemoryTestClass"
	// memoryTestUnloaded and memoryTestLoaded are HOT tenants, registered as
	// lazy shards when the index starts.
	memoryTestUnloaded = "unloaded"
	memoryTestLoaded   = "loaded"
	// memoryTestNever is COLD from the start, so the index creates no directory
	// for it.
	memoryTestNever = "never-loaded"
	// memoryTestMinMMapSize is large enough that some segments are read whole.
	memoryTestMinMMapSize   = 4 * 1024
	memoryTestObjects       = 20
	memoryTestDimensions    = 32
	memoryTestTextProperty  = "name"
	memoryTestVectorDefault = "vec"
)

func TestIndexEstimateShardMemory(t *testing.T) {
	ctx := context.Background()
	hnswVector := func(mutate func(*enthnsw.UserConfig)) map[string]models.VectorConfig {
		cfg := enthnsw.NewDefaultUserConfig()
		mutate(&cfg)
		return map[string]models.VectorConfig{
			memoryTestVectorDefault: {VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg},
		}
	}
	flatVector := func(mutate func(*entflat.UserConfig)) map[string]models.VectorConfig {
		cfg := entflat.NewDefaultUserConfig()
		mutate(&cfg)
		return map[string]models.VectorConfig{
			memoryTestVectorDefault: {VectorIndexType: cfg.IndexType(), VectorIndexConfig: cfg},
		}
	}
	dynamicCfg := dynamicent.NewDefaultUserConfig()
	dynamicVector := map[string]models.VectorConfig{
		memoryTestVectorDefault: {VectorIndexType: dynamicCfg.IndexType(), VectorIndexConfig: dynamicCfg},
	}
	textOnly := []*models.Property{textProperty(memoryTestTextProperty)}
	withData := memoryTestParams{
		properties: textOnly, trackDimensions: true,
		vectors: hnswVector(func(*enthnsw.UserConfig) {}), writeVectors: true,
	}

	t.Run("loaded shard", func(t *testing.T) {
		h := newMemoryTestIndex(ctx, t, withData)
		shard := h.load(ctx, t, memoryTestLoaded)
		h.putObjects(ctx, t, shard)

		require.True(t, h.lazyShard(t, memoryTestLoaded).isLoaded())
		result, err := h.index.EstimateShardMemory(ctx, memoryTestLoaded)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Positive(t, result.DataBytes)
	})

	t.Run("unloaded lazy shard stays unloaded", func(t *testing.T) {
		h := newMemoryTestIndex(ctx, t, withData)
		h.putObjects(ctx, t, h.load(ctx, t, memoryTestUnloaded))
		h.unload(ctx, t, memoryTestUnloaded)

		lazyShard := h.lazyShard(t, memoryTestUnloaded)
		require.False(t, lazyShard.isLoaded())
		result, err := h.index.EstimateShardMemory(ctx, memoryTestUnloaded)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.False(t, lazyShard.isLoaded(), "the estimate must not load the shard")
	})

	t.Run("same figure loaded and unloaded", func(t *testing.T) {
		h := newMemoryTestIndex(ctx, t, withData)
		h.putObjects(ctx, t, h.load(ctx, t, memoryTestUnloaded))
		h.unload(ctx, t, memoryTestUnloaded)
		// The second load writes the sidecar files of the segments the first
		// unload flushed. Both estimates below then see the same files.
		h.load(ctx, t, memoryTestUnloaded)
		h.unload(ctx, t, memoryTestUnloaded)

		lazyShard := h.lazyShard(t, memoryTestUnloaded)
		require.False(t, lazyShard.isLoaded())
		require.False(t, shardusage.ComputedUsageDataExists(h.index.path(), memoryTestUnloaded))
		unloaded, err := h.index.EstimateShardMemory(ctx, memoryTestUnloaded)
		require.NoError(t, err)
		// only the unloaded path saves a usage file
		require.True(t, shardusage.ComputedUsageDataExists(h.index.path(), memoryTestUnloaded))

		h.load(ctx, t, memoryTestUnloaded)
		require.True(t, lazyShard.isLoaded())
		loaded, err := h.index.EstimateShardMemory(ctx, memoryTestUnloaded)
		require.NoError(t, err)

		assert.Positive(t, unloaded.DataBytes)
		assert.Equal(t, unloaded, loaded)
	})

	t.Run("a shard unloaded right after writes reads under until its next load", func(t *testing.T) {
		h := newMemoryTestIndex(ctx, t, withData)
		h.putObjects(ctx, t, h.load(ctx, t, memoryTestUnloaded))
		h.unload(ctx, t, memoryTestUnloaded)

		lazyShard := h.lazyShard(t, memoryTestUnloaded)
		require.False(t, lazyShard.isLoaded())
		require.False(t, shardusage.ComputedUsageDataExists(h.index.path(), memoryTestUnloaded))
		unloadedInputs := h.inputs(ctx, t, memoryTestUnloaded)
		unloaded := shardmemory.Estimate(*unloadedInputs)

		h.load(ctx, t, memoryTestUnloaded)
		require.True(t, lazyShard.isLoaded())
		loadedInputs := h.inputs(ctx, t, memoryTestUnloaded)
		loaded := shardmemory.Estimate(*loadedInputs)

		// The final flush at unload writes segments without their bloom, cna or
		// metadata sidecar files. The next load writes them.
		assert.Less(t, unloadedInputs.BloomBytes, loadedInputs.BloomBytes)
		assert.Less(t, unloaded.DataBytes, loaded.DataBytes)
		assert.Equal(t, unloaded.BaselineBytes, loaded.BaselineBytes)
	})

	t.Run("derived bucket count equals the loaded store's bucket count", func(t *testing.T) {
		on := true
		off := false
		allProperties := func() []*models.Property {
			return []*models.Property{
				textProperty("text"),
				{Name: "int", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: &on},
				{Name: "intArray", DataType: schema.DataTypeIntArray.PropString()},
				{Name: "ref", DataType: []string{"OtherClass"}},
				{Name: "geo", DataType: schema.DataTypeGeoCoordinates.PropString()},
				{Name: "boolean", DataType: schema.DataTypeBoolean.PropString()},
				{Name: "date", DataType: schema.DataTypeDate.PropString()},
				{
					Name: "unindexed", DataType: schema.DataTypeText.PropString(),
					IndexFilterable: &off, IndexSearchable: &off, IndexRangeFilters: &off,
				},
			}
		}
		propertyCase := func(prop *models.Property) memoryTestParams {
			return memoryTestParams{properties: []*models.Property{prop}}
		}
		vectorCase := func(vectors map[string]models.VectorConfig, writeVectors bool) memoryTestParams {
			return memoryTestParams{
				properties: textOnly, trackDimensions: true,
				vectors: vectors, writeVectors: writeVectors,
			}
		}

		tests := []struct {
			name   string
			params memoryTestParams
		}{
			// property types, with every flag off
			{name: "text, filterable and searchable", params: propertyCase(textProperty("text"))},
			{name: "int, filterable and rangeable", params: propertyCase(
				&models.Property{Name: "int", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: &on})},
			{name: "reference, filterable", params: propertyCase(
				&models.Property{Name: "ref", DataType: []string{"OtherClass"}})},
			{name: "geo", params: propertyCase(
				&models.Property{Name: "geo", DataType: schema.DataTypeGeoCoordinates.PropString()})},
			{name: "boolean", params: propertyCase(
				&models.Property{Name: "boolean", DataType: schema.DataTypeBoolean.PropString()})},
			{name: "date", params: propertyCase(
				&models.Property{Name: "date", DataType: schema.DataTypeDate.PropString()})},
			{name: "every index off", params: propertyCase(&models.Property{
				Name: "unindexed", DataType: schema.DataTypeText.PropString(),
				IndexFilterable: &off, IndexSearchable: &off, IndexRangeFilters: &off,
			})},
			{name: "every property type, every flag off", params: memoryTestParams{properties: allProperties()}},
			// one flag on at a time, over every property type
			{name: "IndexNullState on", params: memoryTestParams{properties: allProperties(), indexNullState: true}},
			{
				// the int[] and reference properties get a length bucket
				name:   "IndexPropertyLength on",
				params: memoryTestParams{properties: allProperties(), indexPropertyLength: true},
			},
			{name: "IndexTimestamps on", params: memoryTestParams{properties: allProperties(), indexTimestamps: true}},
			{name: "TrackVectorDimensions on", params: memoryTestParams{properties: allProperties(), trackDimensions: true}},
			// vectors
			{name: "no vector index", params: vectorCase(nil, false)},
			{name: "flat uncompressed", params: vectorCase(flatVector(func(*entflat.UserConfig) {}), true)},
			{name: "flat with BQ", params: vectorCase(flatVector(func(c *entflat.UserConfig) { c.BQ.Enabled = true }), true)},
			{name: "hnsw with Skip", params: vectorCase(hnswVector(func(c *enthnsw.UserConfig) { c.Skip = true; c.BQ.Enabled = true }), false)},
			{name: "hnsw uncompressed", params: vectorCase(hnswVector(func(*enthnsw.UserConfig) {}), true)},
			{name: "hnsw with BQ, empty", params: vectorCase(hnswVector(func(c *enthnsw.UserConfig) { c.BQ.Enabled = true }), false)},
			{name: "hnsw with BQ, with data", params: vectorCase(hnswVector(func(c *enthnsw.UserConfig) { c.BQ.Enabled = true }), true)},
			{name: "hnsw with RQ, empty", params: vectorCase(hnswVector(func(c *enthnsw.UserConfig) { c.RQ.Enabled = true }), false)},
			{name: "hnsw with RQ, with data", params: vectorCase(hnswVector(func(c *enthnsw.UserConfig) { c.RQ.Enabled = true }), true)},
			{name: "hnsw with RQ, with data, 4 dimensions", params: func() memoryTestParams {
				params := vectorCase(hnswVector(func(c *enthnsw.UserConfig) { c.RQ.Enabled = true; c.RQ.Bits = 8 }), true)
				// 8-bit RQ reports a compression ratio below 1 at 4 dimensions
				params.dimensions = 4
				return params
			}()},
			{name: "dynamic below its threshold", params: func() memoryTestParams {
				params := vectorCase(dynamicVector, true)
				// a dynamic index refuses to start without async indexing
				params.asyncIndexing = true
				return params
			}()},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// The unloaded tenant is loaded while the class has none of the
				// case's properties. The class gains them before the count is derived.
				final := tt.params.properties
				tt.params.properties = nil
				h := newMemoryTestIndex(ctx, t, tt.params)
				shard := h.load(ctx, t, memoryTestUnloaded)
				if tt.params.writeVectors {
					h.putObjects(ctx, t, shard)
				}
				h.unload(ctx, t, memoryTestUnloaded)
				h.setProperties(final)

				t.Run("never-loaded tenant", func(t *testing.T) {
					require.NoDirExists(t, shardPath(h.index.path(), memoryTestNever))
					inputs := h.inputs(ctx, t, memoryTestNever)

					shard, release, err := h.index.getOrInitShard(ctx, memoryTestNever)
					require.NoError(t, err)
					defer release()
					requireBucketCount(t, inputs.BucketCount, shard.Store())
				})

				t.Run("unloaded tenant whose class gained a property", func(t *testing.T) {
					lazyShard := h.lazyShard(t, memoryTestUnloaded)
					require.False(t, lazyShard.isLoaded())
					inputs := h.inputs(ctx, t, memoryTestUnloaded)
					require.False(t, lazyShard.isLoaded())
					requireBucketCount(t, inputs.BucketCount, h.load(ctx, t, memoryTestUnloaded).Store())
				})

				t.Run("loaded shard", func(t *testing.T) {
					shard := h.load(ctx, t, memoryTestLoaded)
					if tt.params.writeVectors {
						h.putObjects(ctx, t, shard)
					}
					inputs := h.inputs(ctx, t, memoryTestLoaded)
					requireBucketCount(t, inputs.BucketCount, shard.Store())
				})
			})
		}
	})

	t.Run("never-loaded tenant", func(t *testing.T) {
		h := newMemoryTestIndex(ctx, t, withData)

		require.NoDirExists(t, shardPath(h.index.path(), memoryTestNever))
		inputs := h.inputs(ctx, t, memoryTestNever)
		assert.Zero(t, inputs.BloomBytes)
		assert.Zero(t, inputs.SmallSegmentBytes)
		never, err := h.index.EstimateShardMemory(ctx, memoryTestNever)
		require.NoError(t, err)

		shard, release, err := h.index.getOrInitShard(ctx, memoryTestNever)
		require.NoError(t, err)
		defer release()
		requireBucketCount(t, inputs.BucketCount, shard.Store())
		loaded, err := h.index.EstimateShardMemory(ctx, memoryTestNever)
		require.NoError(t, err)
		assert.Equal(t, never, loaded)
	})

	// unloadedWithSavedUsage returns an index whose unloaded tenant holds data
	// and a usage file saved by a first estimate.
	unloadedWithSavedUsage := func(t *testing.T) *memoryTestIndex {
		h := newMemoryTestIndex(ctx, t, withData)
		h.putObjects(ctx, t, h.load(ctx, t, memoryTestUnloaded))
		h.unload(ctx, t, memoryTestUnloaded)
		_, err := h.index.EstimateShardMemory(ctx, memoryTestUnloaded)
		require.NoError(t, err)
		require.True(t, shardusage.ComputedUsageDataExists(h.index.path(), memoryTestUnloaded))
		return h
	}

	t.Run("cold shard is served from the saved segment heap", func(t *testing.T) {
		h := unloadedWithSavedUsage(t)
		planted := &types.SegmentHeap{BloomBytes: 1111, SmallSegmentBytes: 2222, MinMMapSize: memoryTestMinMMapSize}
		saved := readSavedShardUsage(t, h.index.path(), memoryTestUnloaded)
		saved.SegmentHeap = planted
		writeSavedShardUsage(t, h.index.path(), memoryTestUnloaded, saved)

		inputs := h.inputs(ctx, t, memoryTestUnloaded)
		assert.Equal(t, planted.BloomBytes, inputs.BloomBytes)
		assert.Equal(t, planted.SmallSegmentBytes, inputs.SmallSegmentBytes)
		result, err := h.index.EstimateShardMemory(ctx, memoryTestUnloaded)
		require.NoError(t, err)
		assert.Equal(t, shardmemory.Estimate(*inputs), *result)
	})

	t.Run("old file without a segment heap is recomputed and saved", func(t *testing.T) {
		h := unloadedWithSavedUsage(t)
		saved := readSavedShardUsage(t, h.index.path(), memoryTestUnloaded)
		_, err := shardusage.SaveComputedUsageData(h.index.path(), memoryTestUnloaded, saved.ShardUsage,
			saved.VectorConfigsFingerprint, shardusage.ComputedUsageGeneration(h.index.path(), memoryTestUnloaded))
		require.NoError(t, err)
		old, err := shardusage.LoadComputedUsageData(h.index.path(), memoryTestUnloaded)
		require.NoError(t, err)
		require.Nil(t, old.SegmentHeap)

		inputs := h.inputs(ctx, t, memoryTestUnloaded)

		recomputed, err := shardusage.LoadComputedUsageData(h.index.path(), memoryTestUnloaded)
		require.NoError(t, err)
		require.NotNil(t, recomputed.SegmentHeap)
		assert.Equal(t, h.walkSegmentHeap(t, memoryTestUnloaded), *recomputed.SegmentHeap)
		assert.Equal(t, recomputed.SegmentHeap.BloomBytes, inputs.BloomBytes)
		assert.Equal(t, recomputed.SegmentHeap.SmallSegmentBytes, inputs.SmallSegmentBytes)
	})

	t.Run("changed MinMMapSize is recomputed", func(t *testing.T) {
		h := unloadedWithSavedUsage(t)
		saved := readSavedShardUsage(t, h.index.path(), memoryTestUnloaded)
		saved.SegmentHeap = &types.SegmentHeap{BloomBytes: 1111, SmallSegmentBytes: 2222, MinMMapSize: 2 * memoryTestMinMMapSize}
		writeSavedShardUsage(t, h.index.path(), memoryTestUnloaded, saved)

		inputs := h.inputs(ctx, t, memoryTestUnloaded)

		recomputed, err := shardusage.LoadComputedUsageData(h.index.path(), memoryTestUnloaded)
		require.NoError(t, err)
		require.NotNil(t, recomputed.SegmentHeap)
		assert.Equal(t, h.walkSegmentHeap(t, memoryTestUnloaded), *recomputed.SegmentHeap)
		assert.Equal(t, int64(memoryTestMinMMapSize), recomputed.SegmentHeap.MinMMapSize)
		assert.Equal(t, recomputed.SegmentHeap.BloomBytes, inputs.BloomBytes)
		assert.Equal(t, recomputed.SegmentHeap.SmallSegmentBytes, inputs.SmallSegmentBytes)
	})

	t.Run("missing index", func(t *testing.T) {
		h := newMemoryTestIndex(ctx, t, withData)
		// what getClass returns once the collection is dropped from the schema
		h.class.Store(nil)

		result, err := h.index.EstimateShardMemory(ctx, memoryTestUnloaded)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func textProperty(name string) *models.Property {
	return &models.Property{
		Name:         name,
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	}
}

// requireBucketCount compares a derived bucket count with the buckets a load
// opened, and names them on failure.
func requireBucketCount(t *testing.T, derived int64, store *lsmkv.Store) {
	t.Helper()
	buckets := store.GetBucketsByName()
	require.Equal(t, int64(len(buckets)), derived, "loaded buckets: %v", slices.Sorted(maps.Keys(buckets)))
}

type memoryTestParams struct {
	properties          []*models.Property
	indexNullState      bool
	indexPropertyLength bool
	indexTimestamps     bool
	trackDimensions     bool
	vectors             map[string]models.VectorConfig
	// writeVectors stores memoryTestObjects objects, each with a vector for
	// every configured target vector, wherever a test puts objects.
	writeVectors bool
	// dimensions is the length of every written vector; zero means
	// memoryTestDimensions.
	dimensions    int
	asyncIndexing bool
}

type memoryTestIndex struct {
	index      *Index
	class      atomic.Pointer[models.Class]
	vectors    []string
	dimensions int
}

// newMemoryTestIndex starts a lazy multi-tenant index with the tenants
// memoryTestUnloaded and memoryTestLoaded HOT and memoryTestNever COLD. The
// class the schema returns can be swapped while the index runs.
func newMemoryTestIndex(ctx context.Context, t *testing.T, params memoryTestParams) *memoryTestIndex {
	t.Helper()
	logger, _ := test.NewNullLogger()

	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			memoryTestUnloaded: {Name: memoryTestUnloaded, BelongsToNodes: []string{"test-node"}, Status: models.TenantActivityStatusHOT},
			memoryTestLoaded:   {Name: memoryTestLoaded, BelongsToNodes: []string{"test-node"}, Status: models.TenantActivityStatusHOT},
			memoryTestNever:    {Name: memoryTestNever, BelongsToNodes: []string{"test-node"}, Status: models.TenantActivityStatusCOLD},
		},
		PartitioningEnabled: true,
	}
	shardState.SetLocalName("test-node")

	invertedConfig := &models.InvertedIndexConfig{
		IndexNullState:      params.indexNullState,
		IndexPropertyLength: params.indexPropertyLength,
		IndexTimestamps:     params.indexTimestamps,
	}
	class := &models.Class{
		Class:               memoryTestClass,
		Properties:          params.properties,
		InvertedIndexConfig: invertedConfig,
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
		VectorConfig:        params.vectors,
	}
	h := &memoryTestIndex{dimensions: cmp.Or(params.dimensions, memoryTestDimensions)}
	h.class.Store(class)

	vectorConfigs := make(map[string]schemaConfig.VectorIndexConfig, len(params.vectors))
	for targetVector, vectorConfig := range params.vectors {
		vectorConfigs[targetVector] = vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		if params.writeVectors {
			h.vectors = append(h.vectors, targetVector)
		}
	}

	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, readerFunc func(*models.Class, *sharding.State) error) error {
			return readerFunc(h.class.Load(), shardState)
		}).Maybe()
	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().ReadOnlyClass(memoryTestClass).RunAndReturn(func(string) *models.Class {
		return h.class.Load()
	}).Maybe()
	mockSchema.EXPECT().ShardOwner(memoryTestClass, mock.Anything).Maybe().Return("test-node", nil)
	mockRouter := routerTypes.NewMockRouter(t)

	index, err := NewIndex(ctx, nil, IndexConfig{
		NodeName:                "test-node",
		RootPath:                t.TempDir(),
		ClassName:               schema.ClassName(memoryTestClass),
		ReplicationFactor:       1,
		ShardLoadLimiter:        loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		TrackVectorDimensions:   params.trackDimensions,
		EnableLazyLoadShards:    true,
		HNSWWaitForCachePrefill: true,
		MinMMapSize:             memoryTestMinMMapSize,
	}, inverted.ConfigFromModel(invertedConfig),
		nil,
		vectorConfigs,
		mockRouter,
		resolver.NewShardResolver(memoryTestClass, true, mockSchema),
		mockSchema,
		mockSchemaReader,
		nil,
		logger,
		nil,
		nil,
		&FakeReplicationClient{},
		&replication.GlobalConfig{},
		monitoring.GetMetrics(),
		class,
		nil,
		queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1}),
		memwatch.NewDummyMonitor(),
		NewShardReindexerV3Noop(),
		roaringset.NewBitmapBufPoolNoop(),
		params.asyncIndexing,
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
	h.index = index
	return h
}

// setProperties swaps the class the schema returns for one with properties.
func (h *memoryTestIndex) setProperties(properties []*models.Property) {
	class := *h.class.Load()
	class.Properties = properties
	h.class.Store(&class)
}

func (h *memoryTestIndex) lazyShard(t *testing.T, tenant string) *LazyLoadShard {
	t.Helper()
	lazyShard, ok := asLazyLoadShard(h.index.shards.Load(tenant))
	require.True(t, ok, "tenant %q must be a lazy shard", tenant)
	return lazyShard
}

func (h *memoryTestIndex) load(ctx context.Context, t *testing.T, tenant string) *Shard {
	t.Helper()
	shard, _, err := h.lazyShard(t, tenant).loadIfCold(ctx)
	require.NoError(t, err)
	return shard
}

// unload shuts the lazy shard down, which flushes its memtables, and leaves it
// registered as unloaded.
func (h *memoryTestIndex) unload(ctx context.Context, t *testing.T, tenant string) {
	t.Helper()
	lazyShard := h.lazyShard(t, tenant)
	require.NoError(t, lazyShard.Shutdown(ctx))
	require.False(t, lazyShard.isLoaded())
}

// putObjects writes memoryTestObjects objects to shard, with a vector under
// every target vector of h.vectors and a value for memoryTestTextProperty when
// the class has it.
func (h *memoryTestIndex) putObjects(ctx context.Context, t *testing.T, shard *Shard) {
	t.Helper()
	hasText := slices.ContainsFunc(h.class.Load().Properties, func(p *models.Property) bool {
		return p.Name == memoryTestTextProperty
	})
	for i := range memoryTestObjects {
		obj := &models.Object{
			Class:  memoryTestClass,
			ID:     strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
			Tenant: shard.Name(),
		}
		if hasText {
			obj.Properties = map[string]interface{}{memoryTestTextProperty: fmt.Sprintf("object %d", i)}
		}
		vectors := make(map[string][]float32, len(h.vectors))
		for _, targetVector := range h.vectors {
			vector := make([]float32, h.dimensions)
			for dim := range vector {
				vector[dim] = float32(i*h.dimensions + dim + 1)
			}
			vectors[targetVector] = vector
		}
		require.NoError(t, shard.PutObject(ctx, storobj.FromObject(obj, nil, vectors, nil)))
	}
}

func (h *memoryTestIndex) inputs(ctx context.Context, t *testing.T, tenant string) *shardmemory.Inputs {
	t.Helper()
	inputs, err := h.index.shardMemoryInputs(ctx, tenant)
	require.NoError(t, err)
	require.NotNil(t, inputs)
	return inputs
}

// walkSegmentHeap computes the segment heap of the tenant's files afresh.
func (h *memoryTestIndex) walkSegmentHeap(t *testing.T, tenant string) types.SegmentHeap {
	t.Helper()
	lsmPath := shardPathLSM(h.index.path(), tenant)
	directories, err := diskio.GetSubdirNames(lsmPath)
	require.NoError(t, err)
	heap, err := shardusage.CalculateSegmentHeap(lsmPath, directories, memoryTestMinMMapSize)
	require.NoError(t, err)
	return heap
}
