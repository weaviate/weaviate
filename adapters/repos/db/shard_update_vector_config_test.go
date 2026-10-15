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

// liveEF is the ef the name's hnsw index runs with.
func liveEF(t *testing.T, shard *Shard, name string) int64 {
	t.Helper()
	var ef int64
	found, err := shard.WithVectorIndex(name, func(index VectorIndex) error {
		ef = index.(interface{ EF() int64 }).EF()
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "no index for %q", name)
	return ef
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
	assert.Equal(t, int64(200), liveEF(t, published, "foo"),
		"the shard that was loading runs with the named config from before the update")
	assert.Equal(t, int64(300), liveEF(t, published, ""),
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
	defaultEF := int64(legacy.EF)

	// an update the shard did not see: the index's config moved without a push
	skipped := func(f func()) {
		idx.vectorIndexUserConfigLock.Lock()
		defer idx.vectorIndexUserConfigLock.Unlock()
		f()
	}

	// nothing moved: nothing to catch up with
	shard.catchUpVectorIndexConfig(ctx)
	assert.Equal(t, defaultEF, liveEF(t, shard, "foo"))
	assert.Equal(t, defaultEF, liveEF(t, shard, ""))

	// a named update
	fooUpdated := foo
	fooUpdated.EF = 200
	skipped(func() {
		idx.vectorIndexUserConfigs["foo"] = fooUpdated
		idx.vectorIndexUserConfigGens.targets++
	})
	shard.catchUpVectorIndexConfig(ctx)
	assert.Equal(t, int64(200), liveEF(t, shard, "foo"))
	assert.Equal(t, defaultEF, liveEF(t, shard, ""))

	// a legacy update
	legacyUpdated := legacy
	legacyUpdated.EF = 300
	skipped(func() {
		idx.vectorIndexUserConfig = legacyUpdated
		idx.vectorIndexUserConfigGens.legacy++
	})
	shard.catchUpVectorIndexConfig(ctx)
	assert.Equal(t, int64(300), liveEF(t, shard, ""))
	assert.Equal(t, int64(200), liveEF(t, shard, "foo"))

	// a named vector added
	bar := entflat.NewDefaultUserConfig()
	skipped(func() {
		idx.vectorIndexUserConfigs["bar"] = bar
		idx.vectorIndexUserConfigGens.targets++
	})
	shard.catchUpVectorIndexConfig(ctx)
	found, err := shard.WithVectorIndex("bar", func(VectorIndex) error { return nil })
	require.NoError(t, err)
	assert.True(t, found, "the index added during the load is built at catch-up")
}
