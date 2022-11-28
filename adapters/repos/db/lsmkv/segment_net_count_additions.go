//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ErrInvalidChecksum indicates the the read file should not be trusted. For
// any pre-computed data this is a recoverable issue, as the data can simply be
// re-computed at read-time.
var ErrInvalidChecksum = errors.New("invalid checksum")

// existOnLowerSegments is a simple function that can be passed at segment
// initialization time to check if any of the keys are truly new or previously
// seen. This can in turn be used to build up the net count additions. The
// reason this is abstrate:
type existsOnLowerSegmentsFn func(key []byte) (bool, error)

func (ind *segment) countNetPath() string {
	return countNetPathFromSegmentPath(ind.path)
}

func countNetPathFromSegmentPath(segPath string) string {
	extless := strings.TrimSuffix(segPath, filepath.Ext(segPath))
	return fmt.Sprintf("%s.cna", extless)
}

func (ind *segment) initCountNetAdditions(exists existsOnLowerSegmentsFn) error {
	if ind.strategy != SegmentStrategyReplace {
		// replace is the only strategy that supports counting
		return nil
	}

	ok, err := fileExists(ind.countNetPath())
	if err != nil {
		return err
	}

	if ok {
		err = ind.loadCountNetFromDisk()
		if err == nil {
			return nil
		}

		if err != ErrInvalidChecksum {
			// not a recoverable error
			return err
		}

		// now continue re-calculating
	}

	var lastErr error
	countNet := 0
	cb := func(key []byte, tombstone bool) {
		existedOnPrior, err := exists(key)
		if err != nil {
			lastErr = err
		}

		if tombstone && existedOnPrior {
			countNet--
		}

		if !tombstone && !existedOnPrior {
			countNet++
		}
	}

	extr := newBufferedKeyAndTombstoneExtractor(ind.contents, ind.dataStartPos,
		ind.dataEndPos, 10e6, ind.secondaryIndexCount, cb)

	extr.do()

	ind.countNetAdditions = countNet

	if lastErr != nil {
		return lastErr
	}

	if err := ind.storeCountNetOnDisk(); err != nil {
		return fmt.Errorf("store count net additions on disk: %w", err)
	}

	return nil
}

func (ind *segment) storeCountNetOnDisk() error {
	return storeCountNetOnDisk(ind.countNetPath(), ind.countNetAdditions)
}

// prefillCountNetAdditions is a helper function that can be used in
// compactions. A compacted segment behaves exactly as the two segments it
// replaces. As a result the count net additions of a compacted segment is
// simply the sum of the old two segments.
//
// by "prefilling" which means creating the file on disk, the subsequent
// newSegment() call can skip re-calculating the count net additions which
// would have a high cost on large segment groups.
func prefillCountNetAdditions(segPath string, updatedCountNetAdditions int) error {
	return storeCountNetOnDisk(countNetPathFromSegmentPath(segPath),
		updatedCountNetAdditions)
}

func storeCountNetOnDisk(path string, value int) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, uint64(value)); err != nil {
		return fmt.Errorf("write cna to buf: %w", err)
	}

	return writeWithChecksum(buf.Bytes(), path)
}

func (ind *segment) loadCountNetFromDisk() error {
	data, err := loadWithChecksum(ind.countNetPath(), 12)
	if err != nil {
		return err
	}

	ind.countNetAdditions = int(binary.LittleEndian.Uint64(data[0:8]))

	return nil
}
