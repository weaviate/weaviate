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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestFlushAbort_LeavesWALForReplay covers the correctness contract called
// out by the original bug: a cancelled flush at runtime (e.g. tenant
// deactivate) must not corrupt the bucket. The WAL must survive so a
// subsequent open reconstructs the in-memory state with no data loss.
func TestFlushAbort_LeavesWALForReplay(t *testing.T) {
	strategies := []string{
		StrategyReplace,
		StrategySetCollection,
		StrategyMapCollection,
		StrategyRoaringSet,
		StrategyRoaringSetRange,
		StrategyInverted,
	}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			ctx := context.Background()
			tmpDir := t.TempDir()
			logger, _ := test.NewNullLogger()

			b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy))
			require.NoError(t, err)

			// Seed enough rows that the abort fires well before the inner write
			// loop completes. abortCheckEveryN = 1024, so > 2048 guarantees at
			// least one probe.
			const n = 4096
			seedBucket(t, b, strategy, n)

			// WAL files exist before the flush attempt.
			walsBefore := countFilesWithExt(t, b.GetDir(), ".wal")
			require.Greater(t, walsBefore, 0,
				"sanity: WAL must exist before the aborted flush")

			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			flushErr := b.FlushAndSwitch(cancelled)
			require.Error(t, flushErr,
				"cancelled ctx must surface as an error from FlushAndSwitch")
			require.True(t,
				strings.Contains(flushErr.Error(), context.Canceled.Error()),
				"err should wrap context.Canceled, got: %v", flushErr)

			// .tmp file removed by the deferred cleanup; no .db left behind for
			// the aborted segment.
			tmps := countFilesWithExt(t, b.GetDir(), ".tmp")
			assert.Equal(t, 0, tmps, "partial .db.tmp must be cleaned up")

			// Critical invariant: WAL preserved so reopen replays the data.
			walsAfter := countFilesWithExt(t, b.GetDir(), ".wal")
			assert.Equal(t, walsBefore, walsAfter,
				"WAL must survive an aborted flush — reopen relies on it")

			require.NoError(t, b.Shutdown(ctx))

			// Reopen and assert the seeded data is back via WAL replay.
			b2, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b2.Shutdown(ctx)) })

			assertSeededRows(t, b2, strategy, n)
		})
	}
}

// TestFlushAbort_ReopenAndRetryFlush is the tenant deactivate/reactivate
// scenario: aborted flush, reopen, then a clean flush must succeed and
// produce a usable segment.
func TestFlushAbort_ReopenAndRetryFlush(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)

	const n = 4096
	seedBucket(t, b, StrategyReplace, n)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, b.FlushAndSwitch(cancelled))

	require.NoError(t, b.Shutdown(ctx))

	b2, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b2.Shutdown(ctx)) })

	// A second, uncancelled flush must succeed cleanly and produce a stable
	// .db segment.
	require.NoError(t, b2.FlushAndSwitch(ctx))
	assert.Greater(t, countFilesWithExt(t, b2.GetDir(), ".db"), 0,
		"successful flush after reopen must produce a .db segment")
	assertSeededRows(t, b2, StrategyReplace, n)
}

func seedBucket(t *testing.T, b *Bucket, strategyegy string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		val := []byte(fmt.Sprintf("val-%08d", i))
		switch strategyegy {
		case StrategyReplace:
			require.NoError(t, b.Put(key, val))
		case StrategySetCollection:
			require.NoError(t, b.SetAdd(key, [][]byte{val}))
		case StrategyMapCollection:
			require.NoError(t, b.MapSet(key, MapPair{Key: val, Value: val}))
		case StrategyRoaringSet:
			require.NoError(t, b.RoaringSetAddOne(key, uint64(i)))
		case StrategyRoaringSetRange:
			require.NoError(t, b.RoaringSetRangeAdd(uint64(i), uint64(i)))
		case StrategyInverted:
			require.NoError(t, b.MapSet(key, MapPair{Key: val, Value: val}))
		default:
			t.Fatalf("unhandled strategyegy %q", strategyegy)
		}
	}
}

func assertSeededRows(t *testing.T, b *Bucket, strategyegy string, n int) {
	t.Helper()
	// Sample a few keys; a full sweep is overkill and most strategyegies don't
	// share a uniform Get interface.
	for _, i := range []int{0, n / 2, n - 1} {
		key := []byte(fmt.Sprintf("key-%08d", i))
		val := []byte(fmt.Sprintf("val-%08d", i))
		switch strategyegy {
		case StrategyReplace:
			got, err := b.Get(key)
			require.NoError(t, err)
			assert.Equal(t, val, got, "row %d must survive abort+replay", i)
		case StrategySetCollection:
			got, err := b.SetList(key)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, val, got[0])
		case StrategyMapCollection, StrategyInverted:
			got, err := b.MapList(context.Background(), key)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, val, got[0].Key)
		case StrategyRoaringSet:
			bm, release, err := b.RoaringSetGet(key)
			require.NoError(t, err)
			assert.True(t, bm.Contains(uint64(i)))
			release()
		case StrategyRoaringSetRange:
			// roaringsetrange doesn't expose a direct Get by key; skip the
			// row-level assertion. The WAL-replay assertion via no panic on
			// reopen is sufficient coverage for this strategyegy.
		}
	}
}

func countFilesWithExt(t *testing.T, dir, ext string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	count := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == ext {
			count++
		}
	}
	return count
}
