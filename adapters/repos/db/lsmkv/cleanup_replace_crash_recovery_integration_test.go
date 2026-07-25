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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestCleanupReplace_InPlaceSwitch_CrashRecovery checks that (1) dropping the
// superseded segment does not delete the live .db that now holds the cleaned
// data, and (2) a crash after the switch but before the drop recovers cleanly.
func TestCleanupReplace_InPlaceSwitch_CrashRecovery(t *testing.T) {
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
			opts := []BucketOption{
				WithStrategy(StrategyReplace),
				WithSegmentsCleanupInterval(time.Second),
				WithCalcCountNetAdditions(true),
			}
			if tt.writeInfoInFileName {
				opts = append(opts, WithWriteSegmentInfoIntoFileName(true))
			}

			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.NoError(t, err)
			defer bucket.Shutdown(context.Background())
			bucket.SetMemtableThreshold(1e9)

			// bottom segment: three keys, two of which get superseded above.
			require.NoError(t, bucket.Put([]byte("keyKeep"), []byte("v1")))
			require.NoError(t, bucket.Put([]byte("keyUpdated"), []byte("v1")))
			require.NoError(t, bucket.Put([]byte("keyGone"), []byte("v1")))
			require.NoError(t, bucket.FlushAndSwitch())

			// upper segment: update one key, delete another.
			require.NoError(t, bucket.Put([]byte("keyUpdated"), []byte("v2")))
			require.NoError(t, bucket.Delete([]byte("keyGone")))
			require.NoError(t, bucket.FlushAndSwitch())

			// cleaning the bottom segment removes keyUpdated (superseded) and
			// keyGone (superseded by the tombstone above), keeping keyKeep.
			cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
			require.NoError(t, err)
			require.True(t, cleaned, "expected the bottom segment to be cleaned in place")

			// Capture the on-disk state right after the switch, before the async
			// drop of the superseded segment runs — this is the crash state.
			recoveryDir := t.TempDir()
			copyDir(t, dir, recoveryDir)

			assertState := func(t *testing.T, b *Bucket) {
				v, err := b.Get([]byte("keyKeep"))
				require.NoError(t, err)
				assert.Equal(t, []byte("v1"), v)

				v, err = b.Get([]byte("keyUpdated"))
				require.NoError(t, err)
				assert.Equal(t, []byte("v2"), v)

				v, err = b.Get([]byte("keyGone"))
				require.NoError(t, err)
				assert.Nil(t, v)
			}

			t.Run("drop of superseded segment keeps the live db", func(t *testing.T) {
				_, err := bucket.disk.dropSegmentsAwaiting()
				require.NoError(t, err)
				assertState(t, bucket)
			})

			t.Run("crash-before-drop recovers to the cleaned state", func(t *testing.T) {
				rec, err := NewBucketCreator().NewBucket(ctx, recoveryDir, recoveryDir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				defer rec.Shutdown(context.Background())
				assertState(t, rec)
			})
		})
	}
}

// TestCleanupReplace_InPlaceSwitch_MidSwitchCrashRecovery covers crashes in the
// middle of the switch (between the individual os.Rename calls), hand-crafting
// the on-disk state rather than snapshotting after the switch completes.
func TestCleanupReplace_InPlaceSwitch_MidSwitchCrashRecovery(t *testing.T) {
	ctx := testCtx()

	tests := []struct {
		name                string
		writeInfoInFileName bool
	}{
		{name: "segment info not in filename", writeInfoInFileName: false},
		{name: "segment info in filename", writeInfoInFileName: true},
	}

	// correct end state of both segments; a safely aborted cleanup, and a
	// completed one, must both leave exactly this.
	assertFullState := func(t *testing.T, b *Bucket) {
		v, err := b.Get([]byte("keyKeep"))
		require.NoError(t, err)
		assert.Equal(t, []byte("v1"), v)
		v, err = b.Get([]byte("keyUpdated"))
		require.NoError(t, err)
		assert.Equal(t, []byte("v2"), v)
		v, err = b.Get([]byte("keyGone"))
		require.NoError(t, err)
		assert.Nil(t, v)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []BucketOption{
				WithStrategy(StrategyReplace),
				WithSegmentsCleanupInterval(time.Second),
				WithCalcCountNetAdditions(true),
			}
			if tt.writeInfoInFileName {
				opts = append(opts, WithWriteSegmentInfoIntoFileName(true))
			}

			// writeTwoSegments creates a bucket with a bottom segment (keyKeep,
			// keyUpdated, keyGone) and an upper segment that updates keyUpdated and
			// deletes keyGone, then shuts it down so the state can be manipulated.
			writeTwoSegments := func(t *testing.T) string {
				t.Helper()
				dir := t.TempDir()

				b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				b.SetMemtableThreshold(1e9)

				require.NoError(t, b.Put([]byte("keyKeep"), []byte("v1")))
				require.NoError(t, b.Put([]byte("keyUpdated"), []byte("v1")))
				require.NoError(t, b.Put([]byte("keyGone"), []byte("v1")))
				require.NoError(t, b.FlushAndSwitch())

				require.NoError(t, b.Put([]byte("keyUpdated"), []byte("v2")))
				require.NoError(t, b.Delete([]byte("keyGone")))
				require.NoError(t, b.FlushAndSwitch())

				require.NoError(t, b.Shutdown(context.Background()))
				return dir
			}

			// oldestSegmentDB returns the basename of the oldest on-disk .db segment
			// (ids are fixed-width unix-nano, so the smallest name is oldest).
			oldestSegmentDB := func(t *testing.T, dir string) string {
				t.Helper()
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)

				oldest := ""
				for _, e := range entries {
					name := e.Name()
					if strings.HasPrefix(name, "segment-") && strings.HasSuffix(name, ".db") {
						if oldest == "" || name < oldest {
							oldest = name
						}
					}
				}
				require.NotEmpty(t, oldest, "no on-disk .db segment found")
				return oldest
			}

			// markSegmentSidecarsDeleted renames the sidecars (bloom/CNA/metadata) of
			// the segment owning dbFile to ".deleteme" markers, as
			// markForDeletionExceptSegment does, leaving the .db (and .db.tmp) alone.
			markSegmentSidecarsDeleted := func(t *testing.T, dir, dbFile string) {
				t.Helper()
				prefix := "segment-" + segmentID(dbFile) + "."

				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				for _, e := range entries {
					name := e.Name()
					if name == dbFile || !strings.HasPrefix(name, prefix) ||
						strings.HasSuffix(name, ".tmp") || strings.HasSuffix(name, DeleteMarkerSuffix) {
						continue
					}
					marker := fmt.Sprintf("%s.%013d%s", name, 0, DeleteMarkerSuffix)
					require.NoError(t, os.Rename(filepath.Join(dir, name), filepath.Join(dir, marker)))
				}
			}

			// assertNoTempFiles asserts recovery removed all segment .tmp leftovers.
			assertNoTempFiles := func(t *testing.T, dir string) {
				t.Helper()
				entries, err := os.ReadDir(dir)
				require.NoError(t, err)
				for _, e := range entries {
					if strings.HasPrefix(e.Name(), "segment-") && strings.HasSuffix(e.Name(), ".tmp") {
						t.Errorf("leftover temp file after recovery: %s", e.Name())
					}
				}
			}

			// crash after the sidecars were marked but before the new .db.tmp was
			// renamed onto the old .db: the old .db is still live, the .db.tmp is a
			// leftover. Recovery deletes the .db.tmp and keeps the old data.
			t.Run("leftover .db.tmp with live old .db", func(t *testing.T) {
				dir := writeTwoSegments(t)
				bottomDB := oldestSegmentDB(t, dir)
				copyFile(t, filepath.Join(dir, bottomDB), filepath.Join(dir, bottomDB+".tmp"))
				// a stray precomputed sidecar .tmp must also be swept on recovery
				sidecarTmp := fmt.Sprintf("segment-%s.bloom.tmp", segmentID(bottomDB))
				require.NoError(t, os.WriteFile(filepath.Join(dir, sidecarTmp), []byte("x"), 0o644))
				markSegmentSidecarsDeleted(t, dir, bottomDB)

				b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				defer b.Shutdown(context.Background())
				assertFullState(t, b)

				assertNoTempFiles(t, dir)
			})

			// crash after the .db was overwritten but before the new sidecars were
			// renamed into place: the new .db is live, its canonical sidecars are
			// absent and must be recomputed on load.
			t.Run("new db live, canonical sidecars absent", func(t *testing.T) {
				dir := writeTwoSegments(t)

				b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				b.SetMemtableThreshold(1e9)
				cleaned, err := b.disk.segmentCleaner.cleanupOnce(func() bool { return false })
				require.NoError(t, err)
				require.True(t, cleaned)

				recoveryDir := t.TempDir()
				copyDir(t, dir, recoveryDir)
				require.NoError(t, b.Shutdown(context.Background()))

				markSegmentSidecarsDeleted(t, recoveryDir, oldestSegmentDB(t, recoveryDir))

				rec, err := NewBucketCreator().NewBucket(ctx, recoveryDir, recoveryDir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.NoError(t, err)
				defer rec.Shutdown(context.Background())
				assertFullState(t, rec)
			})
		})
	}
}

// copyDir recursively copies the contents of src into dst (which already exists).
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("cp -a %s/. %s/", src, dst))
	var out bytes.Buffer
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("copy dir: %v: %s", err, out.String())
	}
}
