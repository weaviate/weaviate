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

	sg := &SegmentGroup{
		dir:                     path,
		segments:                segments,
		useBloomFilter:          true,
		mmapContents:            true,
		compactLeftOverSegments: true,
		strategy:                StrategyInverted,
		logger:                  logger,
	}

	for {
		ok, err := sg.convertOnce()
		if err != nil {
			return fmt.Errorf("Error during conversion: %v", err)
		}
		if !ok {
			break
		}
	}
	return nil
}
