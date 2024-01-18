//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCtx() context.Context {
	return context.Background()
}

type bucketIntegrationTest struct {
	name string
	f    func(context.Context, *testing.T, []BucketOption)
	opts []BucketOption
}

type bucketIntegrationTests []bucketIntegrationTest

func (tests bucketIntegrationTests) run(ctx context.Context, t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("mmap", func(t *testing.T) {
				test.f(ctx, t, test.opts)
			})
			t.Run("pread", func(t *testing.T) {
				test.f(ctx, t, append([]BucketOption{WithPread(true)}, test.opts...))
			})
		})
	}
}

func TestCompaction(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "compactionReplaceStrategy",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionReplaceStrategy(ctx, t, opts, 12116, 12116)
			},
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "compactionReplaceStrategy_KeepTombstones",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionReplaceStrategy(ctx, t, opts, 15266, 15266)
			},
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionReplaceStrategy_WithSecondaryKeys",
			f:    compactionReplaceStrategy_WithSecondaryKeys,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
			},
		},
		{
			name: "compactionReplaceStrategy_WithSecondaryKeys_KeepTombstones",
			f:    compactionReplaceStrategy_WithSecondaryKeys,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionReplaceStrategy_RemoveUnnecessaryDeletes",
			f:    compactionReplaceStrategy_RemoveUnnecessaryDeletes,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "compactionReplaceStrategy_RemoveUnnecessaryDeletes_KeepTombstones",
			f:    compactionReplaceStrategy_RemoveUnnecessaryDeletes,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionReplaceStrategy_RemoveUnnecessaryUpdates",
			f:    compactionReplaceStrategy_RemoveUnnecessaryUpdates,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "compactionReplaceStrategy_RemoveUnnecessaryUpdates_KeepTombstones",
			f:    compactionReplaceStrategy_RemoveUnnecessaryUpdates,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionReplaceStrategy_FrequentPutDeleteOperations",
			f:    compactionReplaceStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
			},
		},
		{
			name: "compactionReplaceStrategy_FrequentPutDeleteOperations_KeepTombstones",
			f:    compactionReplaceStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionReplaceStrategy_FrequentPutDeleteOperations_WithSecondaryKeys",
			f:    compactionReplaceStrategy_FrequentPutDeleteOperations_WithSecondaryKeys,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
			},
		},
		{
			name: "compactionReplaceStrategy_FrequentPutDeleteOperations_WithSecondaryKeys_KeepTombstones",
			f:    compactionReplaceStrategy_FrequentPutDeleteOperations_WithSecondaryKeys,
			opts: []BucketOption{
				WithStrategy(StrategyReplace),
				WithSecondaryIndices(1),
				WithKeepTombstones(true),
			},
		},

		{
			name: "compactionSetStrategy",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionSetStrategy(ctx, t, opts, 6836, 6836)
			},
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
			},
		},
		{
			name: "compactionSetStrategy_KeepTombstones",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionSetStrategy(ctx, t, opts, 9756, 9756)
			},
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionSetStrategy_RemoveUnnecessary",
			f:    compactionSetStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
			},
		},
		{
			name: "compactionSetStrategy_RemoveUnnecessary_KeepTombstones",
			f:    compactionSetStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionSetStrategy_FrequentPutDeleteOperations",
			f:    compactionSetStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
			},
		},
		{
			name: "compactionSetStrategy_FrequentPutDeleteOperations_KeepTombstones",
			f:    compactionSetStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
				WithKeepTombstones(true),
			},
		},

		{
			name: "compactionMapStrategy",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionMapStrategy(ctx, t, opts, 10676, 10676)
			},
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
			},
		},
		{
			name: "compactionMapStrategy_KeepTombstones",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionMapStrategy(ctx, t, opts, 13416, 13416)
			},
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionMapStrategy_RemoveUnnecessary",
			f:    compactionMapStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
			},
		},
		{
			name: "compactionMapStrategy_RemoveUnnecessary_KeepTombstones",
			f:    compactionMapStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionMapStrategy_FrequentPutDeleteOperations",
			f:    compactionMapStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
			},
		},
		{
			name: "compactionMapStrategy_FrequentPutDeleteOperations_KeepTombstones",
			f:    compactionMapStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
				WithKeepTombstones(true),
			},
		},

		{
			name: "compactionRoaringSetStrategy_Random",
			f:    compactionRoaringSetStrategy_Random,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
			},
		},
		{
			name: "compactionRoaringSetStrategy_Random_KeepTombstones",
			f:    compactionRoaringSetStrategy_Random,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionRoaringSetStrategy",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionRoaringSetStrategy(ctx, t, opts, 19168, 19168)
			},
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
			},
		},
		{
			name: "compactionRoaringSetStrategy_KeepTombstones",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionRoaringSetStrategy(ctx, t, opts, 29792, 29792)
			},
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionRoaringSetStrategy_RemoveUnnecessary",
			f:    compactionRoaringSetStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
			},
		},
		{
			name: "compactionRoaringSetStrategy_RemoveUnnecessary_KeepTombstones",
			f:    compactionRoaringSetStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionRoaringSetStrategy_FrequentPutDeleteOperations",
			f:    compactionRoaringSetStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
			},
		},
		{
			name: "compactionRoaringSetStrategy_FrequentPutDeleteOperations_KeepTombstones",
			f:    compactionRoaringSetStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyRoaringSet),
				WithKeepTombstones(true),
			},
		},
	}
	tests.run(ctx, t)
}

func nullLogger() logrus.FieldLogger {
	log, _ := test.NewNullLogger()
	return log
}

func copyByteSlice(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func assertSingleSegmentOfSize(t *testing.T, bucket *Bucket, expectedMinSize, expectedMaxSize int64) {
	files, err := bucket.ListFiles(context.Background(), bucket.dir)
	require.NoError(t, err)

	dbFiles := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Ext(f) == ".db" {
			dbFiles = append(dbFiles, f)
		}
	}
	require.Len(t, dbFiles, 1)

	fi, err := os.Stat(dbFiles[0])
	require.NoError(t, err)
	assert.LessOrEqual(t, expectedMinSize, fi.Size())
	assert.GreaterOrEqual(t, expectedMaxSize, fi.Size())
}

func assertSecondSegmentOfSize(t *testing.T, bucket *Bucket, expectedMinSize, expectedMaxSize int64) {
	files, err := bucket.ListFiles(context.Background(), bucket.dir)
	require.NoError(t, err)

	dbFiles := make([]string, 0, len(files))
	for _, f := range files {
		if filepath.Ext(f) == ".db" {
			dbFiles = append(dbFiles, f)
		}
	}
	require.Len(t, dbFiles, 2)

	fi, err := os.Stat(dbFiles[1])
	require.NoError(t, err)
	assert.LessOrEqual(t, expectedMinSize, fi.Size())
	assert.GreaterOrEqual(t, expectedMaxSize, fi.Size())
}
