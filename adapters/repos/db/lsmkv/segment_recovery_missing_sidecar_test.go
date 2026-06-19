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

package lsmkv

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
)

// TestSegment_RecoverFromMissingSidecar reproduces the recovery failure
// observed after ungraceful pod shutdown in
// https://github.com/weaviate/0-weaviate-issues/issues/208 and pins the
// expected behaviour: when a sidecar file (.cna, .bloom, .metadata) is
// listed in the directory snapshot built on bucket open but is missing
// from disk by the time newSegment opens it, the segment must still
// initialise by recomputing the sidecar from the underlying .db segment.
//
// Before the fix, this test fails with "no such file or directory" and
// the segment cannot be loaded; in production this manifests as a
// permanently broken class on the affected node and DEADLINE_EXCEEDED
// gRPC errors at the client.
func TestSegment_RecoverFromMissingSidecar(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	existsOnLower := func(key []byte) (bool, error) { return false, nil }

	cases := []struct {
		name       string
		suffix     string
		bucketOpts []BucketOption
		segCfg     segmentConfig
	}{
		{
			name:   "missing .cna",
			suffix: ".cna",
			bucketOpts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithPread(true),
				WithUseBloomFilter(true),
				WithCalcCountNetAdditions(true),
				WithSegmentsChecksumValidationEnabled(false),
			},
			segCfg: segmentConfig{
				useBloomFilter:        true,
				calcCountNetAdditions: true,
			},
		},
		{
			name:   "missing primary .bloom",
			suffix: ".bloom",
			bucketOpts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithPread(true),
				WithUseBloomFilter(true),
				WithCalcCountNetAdditions(true),
				WithSegmentsChecksumValidationEnabled(false),
			},
			segCfg: segmentConfig{
				useBloomFilter:        true,
				calcCountNetAdditions: true,
			},
		},
		{
			name:   "missing .metadata",
			suffix: ".metadata",
			bucketOpts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithPread(true),
				WithUseBloomFilter(true),
				WithCalcCountNetAdditions(true),
				WithSegmentsChecksumValidationEnabled(false),
				WithWriteMetadata(true),
			},
			segCfg: segmentConfig{
				useBloomFilter:        true,
				calcCountNetAdditions: true,
				writeMetadata:         true,
			},
		},
	}

	const expectedCNA = 2

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()

			// Create real .db plus sidecar files via a flushed bucket.
			func() {
				b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), c.bucketOpts...)
				require.NoError(t, err)
				defer b.Shutdown(ctx)
				require.NoError(t, b.Put([]byte("k1"), []byte("v1")))
				require.NoError(t, b.Put([]byte("k2"), []byte("v2")))
				require.NoError(t, b.FlushMemtable())
			}()

			// Snapshot the directory the same way segment_group does on
			// bucket open. Capture this *before* deleting the sidecar so the
			// stale list still references the now-missing file.
			fileList, _, err := diskio.GetFileWithSizes(dir)
			require.NoError(t, err)

			var dbName, sidecarName string
			for name := range fileList {
				switch {
				case strings.HasSuffix(name, ".db") && dbName == "":
					dbName = name
				case strings.HasSuffix(name, c.suffix) && sidecarName == "":
					sidecarName = name
				}
			}
			require.NotEmpty(t, dbName, "expected a .db segment to be created")
			require.NotEmpty(t, sidecarName, "expected a %s sidecar in the directory listing", c.suffix)

			// Simulate the TOCTOU window: file was present at scan time, then
			// vanishes before newSegment opens it. This is the surface
			// behaviour observed after an ungraceful shutdown where the
			// dirent is replayed but the inode/contents are not.
			require.NoError(t, os.Remove(filepath.Join(dir, sidecarName)))

			cfg := c.segCfg
			cfg.fileList = fileList

			seg, err := newSegment(filepath.Join(dir, dbName), logger, nil, existsOnLower, cfg)
			require.NoError(t, err, "expected segment init to recover from missing %s sidecar; without the fix this fails with ENOENT", c.suffix)
			t.Cleanup(func() { _ = seg.close() })

			_, err = os.Stat(filepath.Join(dir, sidecarName))
			assert.NoError(t, err, "expected %s sidecar to be recreated after recovery", c.suffix)

			assert.Equal(t, expectedCNA, seg.getCountNetAdditions(),
				"expected count net additions to be recomputed correctly after recovery")
		})
	}
}
