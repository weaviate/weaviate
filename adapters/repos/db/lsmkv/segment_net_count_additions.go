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
	extless := strings.TrimSuffix(ind.path, filepath.Ext(ind.path))
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

	// before := time.Now()
	// defer func() {
	// 	fmt.Printf("init count net additions took %s\n", time.Since(before))
	// }()

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
	f, err := os.Create(ind.countNetPath())
	if err != nil {
		return fmt.Errorf("open file for writing: %w", err)
	}

	if err := binary.Write(f, binary.LittleEndian, int64(ind.countNetAdditions)); err != nil {
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
