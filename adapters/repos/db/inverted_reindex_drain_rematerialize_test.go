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
	stderrors "errors"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestModeADrainRematerialize deterministically reproduces the race where a
// cancelled-but-still-draining reindex worker re-enters OnAfterLsmInitAsync
// after a concurrent DELETE: Index.drop renamed the class dir away, but
// pathLSM() re-resolves to the ORIGINAL path and an unguarded tracker
// MkdirAll re-creates it — an orphaned class dir the DELETE was supposed to
// remove. The TEST-ONLY reindexTrackerInitHook parks the worker just before
// the MkdirAll until drop() completes. With the guarded factory init bails
// with context.Canceled and the dir stays absent; reverting the drain loop
// to the unguarded factory turns this test red at the final os.Stat.
func TestModeADrainRematerialize(t *testing.T) {
	ctx := testCtx()
	className := "ModeADrain_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	idxPath := idx.path()
	require.DirExists(t, idxPath, "class dir must exist before drop")

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// The worker signals inHook at the top of init(), then blocks on
	// releaseHook until drop() has completed — no sleeps.
	inHook := make(chan struct{})
	releaseHook := make(chan struct{})
	var hookOnce sync.Once

	reindexTrackerInitHook = func() {
		hookOnce.Do(func() { close(inHook) })
		<-releaseHook
	}
	t.Cleanup(func() { reindexTrackerInitHook = nil })

	var initErr error
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		initErr = buildDrainTracker(task, shard)
	}()

	// Worker parked pre-MkdirAll; drive the DELETE to completion.
	<-inHook
	require.NoError(t, idx.drop())
	require.NoFileExists(t, idxPath,
		"drop() must have renamed the class dir away before the worker proceeds")

	// Release the worker so its MkdirAll runs AFTER the rename.
	close(releaseHook)
	<-workerDone

	if initErr != nil {
		assert.Truef(t, stderrors.Is(initErr, context.Canceled),
			"drain tracker init failed with an unexpected error (want context.Canceled): %v", initErr)
	}

	// The load-bearing assertion.
	_, statErr := os.Stat(idxPath)
	assert.Truef(t, os.IsNotExist(statErr),
		"draining reindex worker re-materialized class dir %q after DELETE renamed it away (stat err: %v)",
		idxPath, statErr)
}

// buildDrainTracker builds the tracker exactly as the production drain loop
// does, so reverting that callsite to the unguarded factory turns the test red.
func buildDrainTracker(task *ShardReindexTaskGeneric, shard ShardLike) error {
	_, err := task.newReindexTrackerGuarded(shard)
	return err
}
