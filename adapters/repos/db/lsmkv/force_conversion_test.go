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
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// This test is designed as a PoC to convert segments from MapCollection to Inverted format, so it should be skipped and run manually
// TODO: migrate it to a proper convertor like compaction or tombstone cleanup
func TestSegmentGroupConverInverted(t *testing.T) {
	path := os.Getenv("PATH_TO_SEGMENTS_TO_CONVERT")
	if path == "" {
		t.Skip("Skipping test because PATH_TO_SEGMENTS_TO_CONVERT is not set")
	}
	err := ConvertSegments(path)
	if err != nil {
		t.Errorf("Error converting segments: %v", err)
	}
}

func ConvertSegments(path string) error {
	logger, _ := test.NewNullLogger()
	// load all segments from folder in disk
	dir, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("Error reading folder: %v", err)
	}

	segments := make([]*segment, 0)

	ctx := context.Background()

	for _, file := range dir {
		segPath := path + "/" + file.Name()
		if strings.HasSuffix(file.Name(), ".db") {
			segment, err := newSegment(segPath, logger, nil, nil, true, true, false, true)
			if err != nil {
				return fmt.Errorf("Error creating segment: %v", err)
			}
			if segment.strategy == segmentindex.StrategyMapCollection && strings.Contains(segPath, "_searchable") {
				segments = append(segments, segment)
			}

		}
	}

	// get dir by first splitting the path to get the parent
	pathSplit := strings.Split(path, "/")
	objectParent := pathSplit[len(pathSplit)-1]
	propName := strings.Split(pathSplit[len(pathSplit)-1], "_")[1]

	opts := []BucketOption{
		WithStrategy(StrategyMapCollection),
		WithPread(true),
		WithKeepTombstones(true),
		WithDynamicMemtableSizing(1, 2, 1, 4),
		WithDirtyThreshold(time.Duration(60)),
		WithAllocChecker(nil),
		WithMaxSegmentSize(math.MaxInt64),
		WithSegmentsCleanupInterval(time.Duration(60)),
	}

	currBucket, err := NewBucketCreator().NewBucket(ctx, path, objectParent, logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	if err != nil {
		return fmt.Errorf("Error creating current bucket: %v", err)
	}

	sg := &SegmentGroup{
		dir:                     path,
		segments:                segments,
		useBloomFilter:          true,
		mmapContents:            true,
		compactLeftOverSegments: true,
		strategy:                StrategyInverted,
		logger:                  logger,
	}

	opts = []BucketOption{
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(2),
		WithPread(true),
		WithKeepTombstones(true),
		WithDynamicMemtableSizing(1, 2, 1, 4),
		WithDirtyThreshold(time.Duration(60)),
		WithAllocChecker(nil),
		WithMaxSegmentSize(math.MaxInt64),
		WithSegmentsCleanupInterval(time.Duration(60)),
	}

	// object path is at objects/
	objectBucketDir := objectParent + "/objects"

	objectBucket, err := NewBucketCreator().NewBucket(ctx, objectBucketDir, objectParent, logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	if err != nil {
		return fmt.Errorf("Error creating object bucket: %v", err)
	}

	idBucketDir := objectParent + "/property__id"

	idBucket, err := NewBucketCreator().NewBucket(ctx, idBucketDir, objectParent, logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	if err != nil {
		return fmt.Errorf("Error creating object bucket: %v", err)
	}

	propTokenization := "word"

	for {
		ok, err := sg.convertOnce(objectBucket, currBucket, idBucket, propName, propTokenization)
		if err != nil {
			return fmt.Errorf("Error during conversion: %v", err)
		}
		if !ok {
			break
		}
	}
	return nil
}
