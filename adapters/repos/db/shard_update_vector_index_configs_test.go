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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins down the RAFT-replay regression: when every target vector is already
// compressed, UpdateVectorIndexConfigs must not flip the shard READONLY and
// rely on a goroutine to flip it back. The pre-fix code spawned that goroutine
// on the first call, then short-circuited on every subsequent call (isReadOnly
// guard) without spawning a replacement — so if the original goroutine had not
// been scheduled by the end of the replay sequence, the shard stayed READONLY.
// Looping 50 iterations is enough to expose any reintroduction: each iteration
// asserts the shard is READY before the next call runs, so even a brief
// READONLY window between calls fails the test.
func TestShard_UpdateVectorIndexConfigs_AllCompressed_SkipsReadonlyCycle(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"

	shd, idx := testShardWithSettings(
		t, ctx,
		&models.Class{Class: className},
		enthnsw.UserConfig{Skip: true},
		false, false, false,
		func(i *Index) {
			// Two named vectors so the migrator's single-vector branch is
			// skipped and the multi-vector path is exercised. Skip:true keeps
			// the init lightweight (noop indexes), which we immediately
			// replace with mocks below.
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"foo": enthnsw.UserConfig{Skip: true},
				"bar": enthnsw.UserConfig{Skip: true},
			}
		},
	)
	defer func() {
		require.Nil(t, idx.drop())
		require.Nil(t, os.RemoveAll(idx.Config.RootPath))
	}()

	// testShardWithSettings disables lazy loading, so the returned ShardLike
	// is the concrete *Shard. We need it directly to swap entries in the
	// (unexported) vectorIndexes map.
	actualShard, ok := shd.(*Shard)
	require.True(t, ok, "expected *Shard, got %T", shd)

	newCompressedMock := func() *MockVectorIndex {
		m := NewMockVectorIndex(t)
		// The hot-path methods we must observe: Compressed()==true forces the
		// new short-circuit, and UpdateUserConfig must invoke its callback
		// synchronously — exactly what the real HNSW does when already
		// compressed.
		m.EXPECT().Compressed().Return(true)
		m.EXPECT().UpdateUserConfig(mock.Anything, mock.Anything).
			RunAndReturn(func(_ schemaConfig.VectorIndexConfig, cb func()) error {
				cb()
				return nil
			})
		// Lifecycle calls fired by idx.drop() at the end of the test —
		// tolerated, not required.
		m.EXPECT().Flush().Return(nil).Maybe()
		m.EXPECT().Shutdown(mock.Anything).Return(nil).Maybe()
		m.EXPECT().Drop(mock.Anything, mock.Anything).Return(nil).Maybe()
		return m
	}

	actualShard.vectorIndexMu.Lock()
	actualShard.vectorIndexes["foo"] = newCompressedMock()
	actualShard.vectorIndexes["bar"] = newCompressedMock()
	actualShard.vectorIndexMu.Unlock()

	require.Equal(t, storagestate.StatusReady, actualShard.GetStatus(),
		"precondition: shard must be READY before the replay loop")
	reasonBefore := actualShard.GetStatusReason()

	updated := map[string]schemaConfig.VectorIndexConfig{
		"foo": enthnsw.UserConfig{},
		"bar": enthnsw.UserConfig{},
	}
	for i := 0; i < 50; i++ {
		require.NoError(t, actualShard.UpdateVectorIndexConfigs(ctx, updated),
			"iteration %d: UpdateVectorIndexConfigs returned an error", i)
		require.Equal(t, storagestate.StatusReady, actualShard.GetStatus(),
			"iteration %d: shard must remain READY (no READONLY/READY cycle when all indexes are already compressed)", i)
	}

	// Reason untouched means neither SetStatusReadonly nor the trailing
	// UpdateStatus(READY) ran — both stamp the same "UpdateVectorIndexConfigs"
	// reason, so a single substring check covers both.
	require.False(t, strings.Contains(actualShard.GetStatusReason(), "UpdateVectorIndexConfigs"),
		"status reason was rewritten by SetStatusReadonly / UpdateStatus(READY); the all-compressed branch should not touch status. before=%q after=%q",
		reasonBefore, actualShard.GetStatusReason())
}
