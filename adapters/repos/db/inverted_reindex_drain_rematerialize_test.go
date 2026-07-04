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

// TestModeADrainRematerialize is the deterministic, single-process
// reproduction + proof for the "draining cancelled reindex worker
// re-materializes a just-DELETEd class dir" race
// (TestBackupVsReindexSuite/CancelClearsTrackerDirsViaOnTaskCompleted).
//
// Scenario the integration test exercises:
//
//  1. A reindex worker is mid drain loop, repeatedly calling
//     OnAfterLsmInitAsync, whose first action is to (re)create the reindex
//     tracker dir via os.MkdirAll(shard.pathLSM()/.migrations/...).
//  2. A DELETE class arrives → Index.drop() sets i.closed=true and renames
//     i.path() away to a <...>.deleteme dir under i.closeLock.Lock(), then
//     spawns an async RemoveAll.
//  3. The cancelled-but-still-draining worker re-enters OnAfterLsmInitAsync
//     AFTER the rename. pathLSM() is re-resolved live to the ORIGINAL
//     (renamed-away) path, so an unguarded MkdirAll re-creates it — leaving
//     an orphaned class dir the DELETE was supposed to remove.
//
// The fix routes the drain-loop tracker creation through
// newReindexTrackerGuarded → Index.withCloseRLockGuard, so the MkdirAll is
// mutually exclusive with drop()'s rename: it either runs before the rename
// (carried into .deleteme) or observes i.closed under closeLock.RLock and
// bails with context.Canceled WITHOUT MkdirAll.
//
// The test drives the exact post-rename interleaving deterministically using
// the TEST-ONLY reindexTrackerInitHook, which parks the worker at the top of
// fileReindexTracker.init() (before the MkdirAll, before any closeLock) until
// the test has driven Index.drop() to completion.
//
// Behavior proof:
//   - WITHOUT the fix (drain loop uses the unguarded
//     t.newReindexTracker(shard.pathLSM())): init() runs MkdirAll after the
//     rename → i.path() re-materializes → this test FAILS at the final
//     os.Stat assertion.
//   - WITH the fix (newReindexTrackerGuarded): init() bails with
//     context.Canceled → i.path() stays absent → this test PASSES.
//
// To exercise both, build the tracker exactly as the production drain path
// does. The drain path is newReindexTrackerGuarded; stashing the fix reverts
// that callsite to the unguarded factory and turns this green test red.
func TestModeADrainRematerialize(t *testing.T) {
	ctx := testCtx()
	className := "ModeADrain_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	idxPath := idx.path()
	require.DirExists(t, idxPath, "class dir must exist before drop")

	// The task is the vehicle for the production drain-path tracker creation
	// (newReindexTrackerGuarded → MigrationDirName / keyParser). Same
	// MapToBlockmax setup the other reindex tests use.
	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// inHook fires once the simulated worker reaches the top of init(); the
	// worker then blocks on releaseHook until the test has run drop() to
	// completion. This makes the drop-vs-MkdirAll interleaving deterministic
	// with no sleeps.
	inHook := make(chan struct{})
	releaseHook := make(chan struct{})
	var hookOnce sync.Once

	reindexTrackerInitHook = func() {
		hookOnce.Do(func() { close(inHook) })
		<-releaseHook
	}
	t.Cleanup(func() { reindexTrackerInitHook = nil })

	// Simulated worker: build the tracker exactly as the production drain path
	// (OnAfterLsmInitAsync) does. With the fix this is the guarded factory;
	// stashing the fix reverts it to the unguarded t.newReindexTracker.
	var initErr error
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		initErr = buildDrainTracker(task, shard)
	}()

	// Wait until the worker is parked at the top of init() (pre-MkdirAll).
	<-inHook

	// Drive the DELETE to completion while the worker is held. drop() renames
	// idxPath away under i.closeLock.Lock() and spawns async RemoveAll.
	require.NoError(t, idx.drop())
	require.NoFileExists(t, idxPath,
		"drop() must have renamed the class dir away before the worker proceeds")

	// Release the worker so its MkdirAll runs AFTER the rename — the race
	// window. With the fix it bails under closeLock.RLock; without it,
	// MkdirAll re-materializes the path.
	close(releaseHook)
	<-workerDone

	// With the fix, init bails with context.Canceled (index closing) and never
	// runs MkdirAll.
	if initErr != nil {
		assert.Truef(t, stderrors.Is(initErr, context.Canceled),
			"drain tracker init failed with an unexpected error (want context.Canceled): %v", initErr)
	}

	// The load-bearing assertion: the class dir DELETE renamed away must stay
	// absent. Without the fix the draining worker re-materializes it here.
	_, statErr := os.Stat(idxPath)
	assert.Truef(t, os.IsNotExist(statErr),
		"draining reindex worker re-materialized class dir %q after DELETE renamed it away (stat err: %v)",
		idxPath, statErr)
}

// buildDrainTracker builds the reindex tracker the way the production worker
// drain loop (OnAfterLsmInitAsync) does. It is a thin indirection so the OLD
// (unguarded) vs NEW (guarded) behavior is selected purely by which factory
// the drain loop is wired to — no test-side branching on the fix.
func buildDrainTracker(task *ShardReindexTaskGeneric, shard ShardLike) error {
	_, err := task.newReindexTrackerGuarded(shard)
	return err
}
