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

package lsmkv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestFlushAfterCommitLogCloseFailureSurvivesReopen crash-sims a flush that
// completed despite a commit-log close failure: the segment landed and the
// WAL was deleted as part of that same successful flush, so a copy of the
// directory taken afterward must reopen and read the row straight from the
// segment.
func TestFlushAfterCommitLogCloseFailureSurvivesReopen(t *testing.T) {
	ctx := testCtx()
	dir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)

	realCL, err := newCommitLogger(filepath.Join(b.GetDir(), "segment-custom"), StrategyReplace, 0)
	require.NoError(t, err)
	m := newRealMemtableWithCommitLogger(t, filepath.Join(b.GetDir(), "segment-custom"), StrategyReplace,
		&failNCloseCommitLogger{memtableCommitLogger: realCL, failN: 1})
	b.active = m

	require.NoError(t, b.Put([]byte("key"), []byte("value")))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	recoveryDir := t.TempDir()
	copyDir(t, dir, recoveryDir)

	reopened, err := NewBucketCreator().NewBucket(ctx, recoveryDir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer reopened.Shutdown(ctx)

	v, err := reopened.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), v)
}

// TestFlushAndSwitchRetainedFlushSurvivesReopen crash-sims a retained flush
// that never got a chance to retry: the injected failure returns before
// Memtable.flush() ever runs, so the WAL is untouched. A copy of the
// directory taken right after the failed FlushAndSwitch call must recover
// the retained row through ordinary WAL replay on open.
func TestFlushAndSwitchRetainedFlushSurvivesReopen(t *testing.T) {
	ctx := testCtx()
	dir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	// WriteWAL is what callers actually rely on for durability (Bucket.Put
	// alone only buffers); force it here so the crash-sim below reflects a
	// genuinely acknowledged write, not an artifact of the test never
	// syncing the WAL.
	require.NoError(t, b.WriteWAL())

	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	require.NoError(t, b.Put([]byte("B"), []byte("vB")))
	require.NoError(t, b.WriteWAL())

	recoveryDir := t.TempDir()
	copyDir(t, dir, recoveryDir)
	require.NoError(t, b.Shutdown(ctx))

	reopened, err := NewBucketCreator().NewBucket(ctx, recoveryDir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	defer reopened.Shutdown(ctx)

	vA, err := reopened.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)

	vB, err := reopened.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)
}

// TestFlushRetryPostDurableFailureCrashWindow documents a real gap rather
// than pinning a guarantee: if the segment file becomes unreadable strictly
// after Memtable.flush() already succeeded (WAL closed AND deleted, as the
// last step of that same successful call) and the process is replaced
// before the in-process retry gets to run, there is nothing left to recover
// from — the WAL is gone and the only on-disk copy is corrupt. This test
// pins the actual observed behavior of opening a bucket in that state, so a
// silent behavior change here is caught; it is not a claim that the data is
// recoverable.
func TestFlushRetryPostDurableFailureCrashWindow(t *testing.T) {
	ctx := testCtx()
	dir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &corruptSegmentOnceMemtable{memtable: b.active}
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	recoveryDir := t.TempDir()
	copyDir(t, dir, recoveryDir)
	require.NoError(t, b.Shutdown(ctx))

	// No .wal file survives this state: flush() already deleted it as the
	// last step of its otherwise-successful run, before the corruption was
	// injected on the return path.
	entries, err := os.ReadDir(recoveryDir)
	require.NoError(t, err)
	sawWAL := false
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".wal" {
			sawWAL = true
		}
	}
	require.False(t, sawWAL, "no WAL should survive: flush() deletes it as its last successful step")

	_, err = NewBucketCreator().NewBucket(ctx, recoveryDir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.Error(t, err, "opening a bucket on a corrupt, unrecoverable segment is expected to fail loudly rather than silently drop the row")
}
