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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestSegmentGroupInit_PromotesSurvivingCleanupTmp reproduces a crash inside the
// segment-cleanup on-disk switch. During that switch the live segment (and its
// sidecars) are renamed to ".deleteme" markers via markForDeletion BEFORE the
// rewritten copy is promoted from ".db.tmp" to ".db". If the process dies in
// that window, the only complete copy of the segment is the ".db.tmp".
//
// Startup used to delete every single-ID ".db.tmp" unconditionally and every
// ".deleteme" afterwards, destroying both copies and silently losing all data
// whose newest version lived in that segment. The fix promotes the surviving
// ".db.tmp" when neither a live "segment-X*.db" nor a "segment-X.wal" exists.
func TestSegmentGroupInit_PromotesSurvivingCleanupTmp(t *testing.T) {
	ctx := testCtx()

	tests := []struct {
		name                string
		writeInfoInFileName bool
	}{
		{name: "segment info not in filename", writeInfoInFileName: false},
		{name: "segment info in filename", writeInfoInFileName: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := []BucketOption{WithStrategy(StrategyReplace)}
			if tt.writeInfoInFileName {
				opts = append(opts, WithWriteSegmentInfoIntoFileName(true))
			}

			b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)

			require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
			require.NoError(t, b.Put([]byte("key-2"), []byte("value-2")))
			// flush to a real, fsynced on-disk segment; its WAL is removed here
			require.NoError(t, b.FlushAndSwitch())
			require.NoError(t, b.Shutdown(ctx))

			simulateCleanupSwitchCrash(t, dir)

			b2, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)
			defer b2.Shutdown(ctx)

			v1, err := b2.Get([]byte("key-1"))
			require.NoError(t, err)
			assert.Equal(t, []byte("value-1"), v1)

			v2, err := b2.Get([]byte("key-2"))
			require.NoError(t, err)
			assert.Equal(t, []byte("value-2"), v2)
		})
	}
}

// simulateCleanupSwitchCrash mutates the on-disk state to match a crash between
// markForDeletion and the .tmp promotion of a cleanup switch: the rewritten copy
// survives as "segment-X....db.tmp" while the live segment and its sidecars are
// renamed to ".deleteme" markers.
func simulateCleanupSwitchCrash(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var dbFile string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "segment-") && strings.HasSuffix(e.Name(), ".db") {
			dbFile = e.Name()
			break
		}
	}
	require.NotEmpty(t, dbFile, "expected exactly one on-disk .db segment")

	// the rewritten cleanup copy: a byte-identical, fully-written .db.tmp
	data, err := os.ReadFile(filepath.Join(dir, dbFile))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, dbFile+".tmp"), data, 0o644))

	// markForDeletion renames the segment and all its sidecars to a
	// ".<counter>.deleteme" marker; reproduce that for every segment-X.* file.
	segPrefix := "segment-" + segmentID(dbFile) + "."
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, segPrefix) || strings.HasSuffix(name, ".tmp") {
			continue
		}
		marker := fmt.Sprintf("%s.%013d%s", name, 0, DeleteMarkerSuffix)
		require.NoError(t, os.Rename(filepath.Join(dir, name), filepath.Join(dir, marker)))
	}
}
