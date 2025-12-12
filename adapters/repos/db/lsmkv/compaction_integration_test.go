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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
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
			test.opts = append(test.opts, WithSegmentsChecksumValidationEnabled(false))
			t.Run("mmap", func(t *testing.T) {
				test.f(ctx, t, test.opts)
			})
			//t.Run("pread", func(t *testing.T) {
			//	test.f(ctx, t, append([]BucketOption{WithPread(true)}, test.opts...))
			//})
		})
	}
}

func TestCompaction(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		// Inverted
		{
			name: "compactionInvertedStrategy",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionInvertedStrategy(ctx, t, opts, 8627, 8627)
			},
			opts: []BucketOption{
				WithStrategy(StrategyInverted),
			},
		},
		{
			name: "compactionInvertedStrategy_KeepTombstones",
			f: func(ctx context.Context, t *testing.T, opts []BucketOption) {
				compactionInvertedStrategy(ctx, t, opts, 8931, 8931)
			},
			opts: []BucketOption{
				WithStrategy(StrategyInverted),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionInvertedStrategy_RemoveUnnecessary",
			f:    compactionInvertedStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategyInverted),
			},
		},
		{
			name: "compactionInvertedStrategy_RemoveUnnecessary_KeepTombstones",
			f:    compactionInvertedStrategy_RemoveUnnecessary,
			opts: []BucketOption{
				WithStrategy(StrategyInverted),
				WithKeepTombstones(true),
			},
		},
		{
			name: "compactionInvertedStrategy_FrequentPutDeleteOperations",
			f:    compactionInvertedStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyInverted),
			},
		},
		{
			name: "compactionInvertedStrategy_FrequentPutDeleteOperations_KeepTombstones",
			f:    compactionInvertedStrategy_FrequentPutDeleteOperations,
			opts: []BucketOption{
				WithStrategy(StrategyInverted),
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
	assert.GreaterOrEqual(t, expectedMaxSize+segmentindex.ChecksumSize, fi.Size())
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
	assert.GreaterOrEqual(t, expectedMaxSize+segmentindex.ChecksumSize, fi.Size())
}
