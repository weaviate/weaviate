package lsmkv

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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
		return ind.loadCountNetFromDisk()
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
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open file for writing: %w", err)
	}

	if err := binary.Write(f, binary.LittleEndian, int64(value)); err != nil {
		return fmt.Errorf("write cna to file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close cna file: %w", err)
	}

	return nil
}

func (ind *segment) loadCountNetFromDisk() error {
	f, err := os.Open(ind.countNetPath())
	if err != nil {
		return fmt.Errorf("open file for reading: %w", err)
	}

	var cna int64
	if err := binary.Read(f, binary.LittleEndian, &cna); err != nil {
		return fmt.Errorf("read cna from file: %w", err)
	}
	ind.countNetAdditions = int(cna)

	if err := f.Close(); err != nil {
		return fmt.Errorf("close cna file: %w", err)
	}

	return nil
}
