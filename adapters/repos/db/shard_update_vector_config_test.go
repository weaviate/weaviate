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

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A config update that the index accepted is written to the vector's mapping
// record, so the record says what the index runs with. One the index refused
// leaves the record as it was.
func TestUpdateVectorIndexConfigs_RecordsTheAdoptedConfig(t *testing.T) {
	ctx := context.Background()
	legacy := enthnsw.NewDefaultUserConfig()
	foo := enthnsw.NewDefaultUserConfig()
	foo.RQ.Enabled = true
	foo.RQ.Bits = 8
	class := &models.Class{Class: "AdoptedConfig", VectorConfig: map[string]models.VectorConfig{
		"foo": {VectorIndexType: foo.IndexType(), VectorIndexConfig: foo},
	}}
	shardLike, _ := testShardWithSettings(t, ctx, class, legacy, false, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"foo": foo}
	})
	shard := underlyingShard(t, shardLike)

	// the first load recorded the config each index was built with
	assert.Equal(t, recorded(shard, "foo", "vectors_foo", "ready"), mustRecord(t, shard, "foo"))
	assert.Equal(t, recorded(shard, "", "main", "ready"), mustRecord(t, shard, ""))

	// a named vector's update, accepted: the record follows
	updated := foo
	updated.EF = 200
	require.NoError(t, shard.UpdateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"foo": updated}))
	rec := mustRecord(t, shard, "foo")
	assert.Equal(t, updated, rec.Config)
	assert.Equal(t, "vectors_foo", rec.QueueID)
	assert.Equal(t, "ready", rec.State)

	// the legacy vector's update, accepted: the record follows
	legacyUpdated := legacy
	legacyUpdated.EF = 300
	require.NoError(t, shard.UpdateVectorIndexConfig(ctx, legacyUpdated))
	assert.Equal(t, legacyUpdated, mustRecord(t, shard, "").Config)

	// refused by the index: nothing is recorded
	refused := updated
	refused.RQ.Bits = 1
	err := shard.UpdateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"foo": refused})
	require.ErrorContains(t, err, "rq bits is immutable")
	assert.Equal(t, updated, mustRecord(t, shard, "foo").Config, "a refused config is not the index's config")

	// the same config again writes nothing new
	require.NoError(t, shard.UpdateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"foo": updated}))
	assert.Equal(t, updated, mustRecord(t, shard, "foo").Config)

	// a skipped vector added by an update owns no record to write to
	require.NoError(t, shard.UpdateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{
		"skipped": enthnsw.UserConfig{Skip: true},
	}))
	_, ok, err := shard.mapping.Get("skipped")
	require.NoError(t, err)
	assert.False(t, ok)
}

// mustRecord is name's mapping record, which must exist.
func mustRecord(t *testing.T, shard *Shard, name string) vectorIndexRecord {
	t.Helper()
	rec, ok, err := shard.mapping.Get(name)
	require.NoError(t, err)
	require.True(t, ok, "no record for %q", name)
	return rec
}
