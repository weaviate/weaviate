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

package lsmkv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
)

func TestSegment_StripTmpExtensions(t *testing.T) {
	logger, _ := test.NewNullLogger()
	existsOnLower := func(key []byte) (bool, error) { return false, nil }

	leftSegmentID := "1234567890"
	rightSegmentID := "2345678901"

	type createSegment func(t *testing.T, dir string) *segment
	createSegmentBloomCna := func(segmentFilename string) createSegment {
		bucketOptions := []BucketOption{
			WithStrategy(StrategyReplace),
			WithPread(true),
			WithUseBloomFilter(true),
			WithCalcCountNetAdditions(true),
			WithSegmentsChecksumValidationEnabled(false),
			WithSecondaryIndices(2),
		}
		expectedFileTypes := map[string]int{
			".bloom": 3,
			".cna":   1,
			".db":    1,
		}
		segmentConfig := segmentConfig{
			mmapContents:             false,
			useBloomFilter:           true,
			calcCountNetAdditions:    true,
			enableChecksumValidation: false,
		}

		return func(t *testing.T, dir string) *segment {
			createSegmentFilesUsingBucket(t, context.Background(), logger, dir, segmentFilename, bucketOptions, expectedFileTypes)

			segment, err := newSegment(filepath.Join(dir, segmentFilename), logger, nil, existsOnLower, segmentConfig)
			require.NoError(t, err)

			return segment
		}
	}
	createSegmentMetadata := func(segmentFilename string) createSegment {
		bucketOptions := []BucketOption{
			WithStrategy(StrategyReplace),
			WithPread(true),
			WithUseBloomFilter(true),
			WithCalcCountNetAdditions(true),
			WithSegmentsChecksumValidationEnabled(false),
			WithWriteMetadata(true),
			WithSecondaryIndices(2),
		}
		expectedFileTypes := map[string]int{
			".metadata": 1,
			".db":       1,
		}
		segmentConfig := segmentConfig{
			mmapContents:             false,
			useBloomFilter:           true,
			calcCountNetAdditions:    true,
			writeMetadata:            true,
			enableChecksumValidation: false,
		}

		return func(t *testing.T, dir string) *segment {
			createSegmentFilesUsingBucket(t, context.Background(), logger, dir, segmentFilename, bucketOptions, expectedFileTypes)

			segment, err := newSegment(filepath.Join(dir, segmentFilename), logger, nil, existsOnLower, segmentConfig)
			require.NoError(t, err)

			return segment
		}
	}

	assertFilenames := func(t *testing.T, dir string, expectedFilenames []string) {
		t.Helper()

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)

		filenamesMap := make(map[string]struct{}, len(entries))
		for i := range entries {
			filenamesMap[entries[i].Name()] = struct{}{}
		}

		require.Len(t, filenamesMap, len(expectedFilenames))
		for i := range expectedFilenames {
			assert.Contains(t, filenamesMap, expectedFilenames[i])
		}
	}

	for _, tc := range []struct {
		name              string
		createSegment     createSegment
		expectedFilenames []string
	}{
		{
			name:          "bloom + cna, single id name (left)",
			createSegment: createSegmentBloomCna(fmt.Sprintf("segment-%s.db.tmp", leftSegmentID)),
			expectedFilenames: []string{
				fmt.Sprintf("segment-%s.db", leftSegmentID),
				fmt.Sprintf("segment-%s.cna", leftSegmentID),
				fmt.Sprintf("segment-%s.bloom", leftSegmentID),
				fmt.Sprintf("segment-%s.secondary.0.bloom", leftSegmentID),
				fmt.Sprintf("segment-%s.secondary.1.bloom", leftSegmentID),
			},
		},
		{
			name:          "bloom + cna, single id name (right)",
			createSegment: createSegmentBloomCna(fmt.Sprintf("segment-%s.db.tmp", rightSegmentID)),
			expectedFilenames: []string{
				fmt.Sprintf("segment-%s.db", rightSegmentID),
				fmt.Sprintf("segment-%s.cna", rightSegmentID),
				fmt.Sprintf("segment-%s.bloom", rightSegmentID),
				fmt.Sprintf("segment-%s.secondary.0.bloom", rightSegmentID),
				fmt.Sprintf("segment-%s.secondary.1.bloom", rightSegmentID),
			},
		},
		{
			name:          "bloom + cna, double id name (gets name from right)",
			createSegment: createSegmentBloomCna(fmt.Sprintf("segment-%s_%s.db.tmp", leftSegmentID, rightSegmentID)),
			expectedFilenames: []string{
				fmt.Sprintf("segment-%s.db", rightSegmentID),
				fmt.Sprintf("segment-%s.cna", rightSegmentID),
				fmt.Sprintf("segment-%s.bloom", rightSegmentID),
				fmt.Sprintf("segment-%s.secondary.0.bloom", rightSegmentID),
				fmt.Sprintf("segment-%s.secondary.1.bloom", rightSegmentID),
			},
		},
		{
			name:          "metadata, single id name (left)",
			createSegment: createSegmentMetadata(fmt.Sprintf("segment-%s.db.tmp", leftSegmentID)),
			expectedFilenames: []string{
				fmt.Sprintf("segment-%s.db", leftSegmentID),
				fmt.Sprintf("segment-%s.metadata", leftSegmentID),
			},
		},
		{
			name:          "metadata, single id name (right)",
			createSegment: createSegmentMetadata(fmt.Sprintf("segment-%s.db.tmp", rightSegmentID)),
			expectedFilenames: []string{
				fmt.Sprintf("segment-%s.db", rightSegmentID),
				fmt.Sprintf("segment-%s.metadata", rightSegmentID),
			},
		},
		{
			name:          "metadata, double id name (gets name from right)",
			createSegment: createSegmentMetadata(fmt.Sprintf("segment-%s_%s.db.tmp", leftSegmentID, rightSegmentID)),
			expectedFilenames: []string{
				fmt.Sprintf("segment-%s.db", rightSegmentID),
				fmt.Sprintf("segment-%s.metadata", rightSegmentID),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			segment := tc.createSegment(t, dir)

			err := segment.stripTmpExtensions(leftSegmentID, rightSegmentID)
			require.NoError(t, err)

			assertFilenames(t, dir, tc.expectedFilenames)
		})
	}
}

func createSegmentFilesUsingBucket(t *testing.T, ctx context.Context, logger logrus.FieldLogger, path, segmentFilename string,
	bucketOptions []BucketOption, expectedFileTypes map[string]int,
) {
	t.Helper()

	func() {
		b, err := NewBucketCreator().NewBucket(ctx, path, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), bucketOptions...)
		require.NoError(t, err)
		defer b.Shutdown(ctx)

		err = b.Put([]byte("key"), []byte("value"), WithSecondaryKey(0, []byte("seckey0")), WithSecondaryKey(1, []byte("seckey1")))
		require.NoError(t, err)

		err = b.FlushMemtable()
		require.NoError(t, err)
	}()

	entries, err := os.ReadDir(path)
	require.NoError(t, err)

	var oldSegmentName string
	newSegmentName := "segment-" + segmentID(segmentFilename)

	fileTypes := map[string]int{}
	for i := range entries {
		fileTypes[filepath.Ext(entries[i].Name())] += 1
		if oldSegmentName == "" {
			oldSegmentName = "segment-" + segmentID(entries[i].Name())
		}
	}

	require.Len(t, fileTypes, len(expectedFileTypes))
	for ext, count := range expectedFileTypes {
		require.Contains(t, fileTypes, ext)
		require.Equal(t, count, fileTypes[ext])
	}

	tmpExt := ""
	if filepath.Ext(segmentFilename) == ".tmp" {
		tmpExt = ".tmp"
	}

	for i := range entries {
		oldFilename := entries[i].Name()
		newFilename := strings.ReplaceAll(oldFilename, oldSegmentName, newSegmentName) + tmpExt

		err = os.Rename(filepath.Join(path, oldFilename), filepath.Join(path, newFilename))
		require.NoError(t, err)
	}

	err = diskio.Fsync(path)
	require.NoError(t, err)
}
