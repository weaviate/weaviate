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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	entdynamic "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// storageExistsFor reports whether the directories rec occupies under the
// shard are all on disk.
func storageExistsFor(t *testing.T, s *Shard, rec vectorIndexRecord) bool {
	t.Helper()
	dirs, err := s.vectorIndexStorageDirsFor(rec)
	require.NoError(t, err)
	exists, err := vectorIndexStorageExists(dirs)
	require.NoError(t, err)
	return exists
}

// TestInitShardVectors_FirstLoadWritesTheMapping pins the first load of a
// shard without a mapping: every index is built as before and recorded
// ready, at the ID the naming rule gives, with its storage on disk.
func TestInitShardVectors_FirstLoadWritesTheMapping(t *testing.T) {
	ctx := testCtx()
	shard, _ := setupDropVectorShard(t, ctx)

	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Equal(t, map[string]vectorIndexRecord{
		"":    {PhysicalID: "main", IndexType: "hnsw", State: "ready"},
		"foo": {PhysicalID: "vectors_foo", IndexType: "hnsw", State: "ready"},
		"mv":  {PhysicalID: "vectors_mv", IndexType: "hnsw", State: "ready"},
	}, records)
	for name, rec := range records {
		assert.True(t, storageExistsFor(t, shard, rec), "storage of %q", name)
	}
}

// TestInitShardVectors_FirstLoadRecordsEveryType pins that each index type
// leaves the directories the probe expects behind after its constructor
// ran, so a ready record written at first load is never a false alarm at
// the next one.
func TestInitShardVectors_FirstLoadRecordsEveryType(t *testing.T) {
	flatCfg := entflat.UserConfig{}
	flatCfg.SetDefaults()
	dist := distancer.NewL2SquaredProvider()

	tests := []struct {
		name      string
		cfg       schemaConfig.VectorIndexConfig
		async     bool
		indexType string
	}{
		{name: "hnsw", cfg: enthnsw.NewDefaultUserConfig(), indexType: "hnsw"},
		{name: "flat", cfg: flatCfg, indexType: "flat"},
		{name: "hfresh", cfg: enthfresh.NewDefaultUserConfig(), indexType: "hfresh"},
		{name: "dynamic", indexType: "dynamic", async: true, cfg: entdynamic.UserConfig{
			Threshold: 1_000_000,
			Distance:  dist.Type(),
			HnswUC:    enthnsw.UserConfig{MaxConnections: 8, EFConstruction: 16, EF: 8, VectorCacheMaxObjects: 1000},
			FlatUC:    flatCfg,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shd, _ := testShardWithSettings(t, ctx, &models.Class{Class: "EveryType"}, tt.cfg, false, tt.async)
			shard := underlyingShard(t, shd)

			records, _, err := shard.mapping.Load()
			require.NoError(t, err)
			rec := vectorIndexRecord{PhysicalID: "main", IndexType: tt.indexType, State: "ready"}
			assert.Equal(t, map[string]vectorIndexRecord{"": rec}, records)
			assert.True(t, storageExistsFor(t, shard, rec))
		})
	}
}

// TestInitShardVectors_SkippedIndexHasNoRecord pins that an hnsw vector
// with skip set builds a no-op index that owns no files, and the mapping
// does not record it: the shard is initialized with no records.
func TestInitShardVectors_SkippedIndexHasNoRecord(t *testing.T) {
	ctx := testCtx()
	shd, _ := testShard(t, ctx, "SkippedVector")
	shard := underlyingShard(t, shd)

	records, initialized, err := shard.mapping.Load()
	require.NoError(t, err)
	assert.True(t, initialized)
	assert.Empty(t, records)
}
