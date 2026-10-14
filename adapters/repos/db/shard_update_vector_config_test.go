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
	entflat "github.com/weaviate/weaviate/entities/vectorindex/flat"
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

// A config update that lands while a shard is loading reaches neither the
// shard (it is not published yet, so the push skips it) nor its config
// snapshot (taken before the update wrote the index's config). The index
// writes its config before the push, and a shard catches up with it when it
// is published, so the shard runs with the updated config.
func TestUpdateVectorIndexConfigs_ReachesAShardThatWasLoading(t *testing.T) {
	ctx := context.Background()
	legacy := enthnsw.NewDefaultUserConfig()
	foo := enthnsw.NewDefaultUserConfig()
	class := &models.Class{Class: "LoadingRace", VectorConfig: map[string]models.VectorConfig{
		"foo": {VectorIndexType: foo.IndexType(), VectorIndexConfig: foo},
	}}
	shardLike, idx := testShardWithSettings(t, ctx, class, legacy, false, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"foo": foo}
	})
	shard := underlyingShard(t, shardLike)
	require.NoError(t, shard.Shutdown(ctx))
	idx.shards.LoadAndDelete(shard.Name())

	// the shard loads: it has read its config and built its indexes, but is
	// not published yet
	loading, err := idx.initShard(ctx, shard.Name(), class, nil, true, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = loading.Shutdown(context.Background()) })

	// the updates land in between: they reach the loaded shards (none) and
	// become the index's config
	updated := foo
	updated.EF = 200
	require.NoError(t, idx.updateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{"foo": updated}))
	legacyUpdated := legacy
	legacyUpdated.EF = 300
	require.NoError(t, idx.updateVectorIndexConfig(ctx, legacyUpdated))

	idx.publishShard(shard.Name(), loading)

	published := underlyingShard(t, loading)
	assert.Equal(t, updated, mustRecord(t, published, "foo").Config,
		"the shard that was loading runs with the named config from before the update")
	assert.Equal(t, legacyUpdated, mustRecord(t, published, "").Config,
		"the shard that was loading runs with the legacy config from before the update")
}

// A shard catches up only with the config whose generation moved since its
// load read it: the named configs for a named update, the legacy config for
// a legacy one, and a named vector added meanwhile gets its index.
func TestCatchUpVectorIndexConfig(t *testing.T) {
	ctx := context.Background()
	legacy := enthnsw.NewDefaultUserConfig()
	foo := enthnsw.NewDefaultUserConfig()
	class := &models.Class{Class: "CatchUp", VectorConfig: map[string]models.VectorConfig{
		"foo": {VectorIndexType: foo.IndexType(), VectorIndexConfig: foo},
	}}
	shardLike, idx := testShardWithSettings(t, ctx, class, legacy, false, false, func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"foo": foo}
	})
	shard := underlyingShard(t, shardLike)

	// an update the shard did not see: the index's config moved without a push
	skipped := func(f func()) {
		idx.vectorIndexUserConfigLock.Lock()
		defer idx.vectorIndexUserConfigLock.Unlock()
		f()
	}

	// nothing moved: nothing to catch up with
	shard.catchUpVectorIndexConfig(ctx)
	assert.Equal(t, foo, mustRecord(t, shard, "foo").Config)
	assert.Equal(t, legacy, mustRecord(t, shard, "").Config)

	// a named update
	fooUpdated := foo
	fooUpdated.EF = 200
	skipped(func() {
		idx.vectorIndexUserConfigs["foo"] = fooUpdated
		idx.vectorIndexUserConfigGens.targets++
	})
	shard.catchUpVectorIndexConfig(ctx)
	assert.Equal(t, fooUpdated, mustRecord(t, shard, "foo").Config)
	assert.Equal(t, legacy, mustRecord(t, shard, "").Config)

	// a legacy update
	legacyUpdated := legacy
	legacyUpdated.EF = 300
	skipped(func() {
		idx.vectorIndexUserConfig = legacyUpdated
		idx.vectorIndexUserConfigGens.legacy++
	})
	shard.catchUpVectorIndexConfig(ctx)
	assert.Equal(t, legacyUpdated, mustRecord(t, shard, "").Config)
	assert.Equal(t, fooUpdated, mustRecord(t, shard, "foo").Config)

	// a named vector added
	bar := entflat.NewDefaultUserConfig()
	skipped(func() {
		idx.vectorIndexUserConfigs["bar"] = bar
		idx.vectorIndexUserConfigGens.targets++
	})
	shard.catchUpVectorIndexConfig(ctx)
	rec := mustRecord(t, shard, "bar")
	assert.Equal(t, bar, rec.Config)
	assert.Equal(t, "ready", rec.State)
	found, err := shard.WithVectorIndex("bar", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found, "the index added during the load is built at catch-up")
}
