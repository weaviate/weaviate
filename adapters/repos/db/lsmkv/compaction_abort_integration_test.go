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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCompactor_AbortOnShouldAbort verifies the core contract introduced by
// weaviate/0-weaviate-issues#250: a compactor in flight observes the
// cyclemanager's shouldAbort signal within compactor.AbortCheckEveryN keys
// and bails — returning (false, nil) so the cycle treats it as a no-op
// iteration. No partial .tmp file is left behind.
func TestCompactor_AbortOnShouldAbort(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	// Keep the memtable threshold high so flush only happens when we
	// explicitly call FlushAndSwitch — predictable segment count.
	bucket.SetMemtableThreshold(1e9)

	// Fill two segments so compactOnce has a real pair to merge.
	for seg := 0; seg < 2; seg++ {
		for i := 0; i < 5000; i++ {
			key := []byte(fmt.Sprintf("seg-%d-key-%08d", seg, i))
			val := []byte(fmt.Sprintf("value-%d", i))
			require.NoError(t, bucket.Put(key, val))
		}
		require.NoError(t, bucket.FlushAndSwitch())
	}
	require.GreaterOrEqual(t, len(bucket.disk.segments), 2,
		"need at least two segments on disk to exercise compactOnce")

	// shouldAbort=always-true; the watcher goroutine inside compactOnce
	// cancels the inner ctx within ~50ms (its poll interval), the compactor
	// observes it at its next AbortCheckEveryN sample.
	shouldAbort := func() bool { return true }

	start := time.Now()
	compacted, err := bucket.disk.compactOnce(shouldAbort)
	elapsed := time.Since(start)

	// Abort path: (false, nil) — same shape the cyclemanager sees on any
	// no-op tick. The partial output (.tmp) is cleaned up so it does not
	// linger across cycle ticks.
	require.NoError(t, err)
	assert.False(t, compacted, "compactor must return compacted=false on abort")
	assert.Less(t, elapsed, 3*time.Second,
		"abort should land within a few seconds; observed %s", elapsed)

	// Verify no .tmp file remains in the bucket dir — the aborted
	// compactor's partial output was cleaned up by abortAndCleanup.
	entries, err := os.ReadDir(dirName)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, strings.HasSuffix(e.Name(), ".db.tmp"),
			"aborted compactor left tmp file %q", e.Name())
	}
}
