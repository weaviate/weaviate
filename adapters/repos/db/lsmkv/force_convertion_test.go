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
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// This test is designed as a PoC to convert segments from MapCollection to Inverted format, so it should be skipped and run manually
// TODO: migrate it to a proper convertor like compaction or tombstone cleanup
func TestSegmentGroupConverInverted(t *testing.T) {
	path := os.Getenv("PATH_TO_SEGMENTS_TO_CONVERT")
	if path == "" {
		// path = "/Users/amourao/code/weaviate/weaviate/data-baseline_tmo/msmarco/hywQvb8cbCCI/lsm/property_text_searchable"
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
		return fmt.Errorf("Error reading folder: %w", err)
	}

	segments := make([]*segment, 0)

	for _, file := range dir {
		segPath := path + "/" + file.Name()
		if strings.HasSuffix(file.Name(), ".db") {
			segment, err := newSegment(segPath, logger, nil, nil, segmentConfig{true, true, false, false, false})
			if err != nil {
				return fmt.Errorf("Error creating segment: %w", err)
			}
			if segment.strategy == segmentindex.StrategyMapCollection && strings.Contains(segPath, "_searchable") {
				segments = append(segments, segment)
			}

		}
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

	shouldAbort := func() bool {
		return false
	}

	for {
		ok, last, path, metrics, err := sg.convertOnce(shouldAbort)
		if err != nil {
			return fmt.Errorf("error during conversion: %w", err)
		}
		if !ok {
			break
		}
		if ok {
			sg.logger.WithField("action", "segment_conversion").WithField("metrics", metrics).Infof("Segment %q converted successfully", path)
		}
		if last {
			sg.logger.WithField("action", "segment_conversion").Info("All segments converted successfully")
			break
		}
	}
	return nil
}
