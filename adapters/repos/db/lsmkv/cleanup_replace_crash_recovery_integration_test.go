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
	"os/exec"
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
