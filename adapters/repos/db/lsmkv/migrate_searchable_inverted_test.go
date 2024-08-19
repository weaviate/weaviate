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
	"os"
	"strings"
	"testing"

	logrus "github.com/sirupsen/logrus/hooks/test"
)

func convertSearchableSegmentToInverted(segmentPath string) error {
	logger, _ := logrus.NewNullLogger()

	segment, err := newSegment(segmentPath, logger,
		nil, nil, false, false, false, false)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(segmentPath+".tmp.inverted", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		return err
	}

	c1 := segment.newCollectionCursorReusable()

	scratchSpacePath := segmentPath + "compaction.scratch.d"

	cimic := newConverterMapInvertedCollection(f, c1, scratchSpacePath, false)

	cimic.do()

	f.Sync()

	f.Close()

	return nil
}

func convertSearchableDirToInverted(dir string) error {
	// list all *.db files
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(file.Name(), ".db") {
			continue
		}
		fullPath := dir + "/" + file.Name()
		if err := convertSearchableSegmentToInverted(fullPath); err != nil {
			return err
		}
	}

	return nil
}

func TestConvert(t *testing.T) {
	// get value from env
	dir := os.Getenv("SEGMENT_DIR_TO_CONVERT")
	if dir == "" {
		t.Skip("no SEGMENT_DIR_TO_CONVERT env var set")
	}
	convertSearchableDirToInverted(dir)
}
