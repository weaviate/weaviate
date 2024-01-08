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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// ErrInvalidChecksum indicates that the read file should not be trusted. For
// any pre-computed data this is a recoverable issue, as the data can simply be
// re-computed at read-time.
var ErrInvalidChecksum = errors.New("invalid checksum")

// existOnLowerSegments is a simple function that can be passed at segment
// initialization time to check if any of the keys are truly new or previously
// seen. This can in turn be used to build up the net count additions. The
// reason this is abstract:
type existsOnLowerSegmentsFn func(key []byte) (bool, error)

func (s *segment) countNetPath() string {
	return countNetPathFromSegmentPath(s.path)
}

func countNetPathFromSegmentPath(segPath string) string {
	extless := strings.TrimSuffix(segPath, filepath.Ext(segPath))
	return fmt.Sprintf("%s.cna", extless)
}

func (s *segment) initCountNetAdditions(exists existsOnLowerSegmentsFn) error {
	if s.strategy != segmentindex.StrategyReplace {
		// replace is the only strategy that supports counting
		return nil
	}

	ok, err := fileExists(s.countNetPath())
	if err != nil {
		return err
	}

	if ok {
		err = s.loadCountNetFromDisk()
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

	extr := newBufferedKeyAndTombstoneExtractor(s.contents, s.dataStartPos,
		s.dataEndPos, 10e6, s.secondaryIndexCount, cb)

	extr.do()

	s.countNetAdditions = countNet

	if lastErr != nil {
		return lastErr
	}

	if err := s.storeCountNetOnDisk(); err != nil {
		return fmt.Errorf("store count net additions on disk: %w", err)
	}

	return nil
}

func (s *segment) storeCountNetOnDisk() error {
	return storeCountNetOnDisk(s.countNetPath(), s.countNetAdditions)
}

func storeCountNetOnDisk(path string, value int) error {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, uint64(value)); err != nil {
		return fmt.Errorf("write cna to buf: %w", err)
	}

	return writeWithChecksum(buf.Bytes(), path)
}

func (s *segment) loadCountNetFromDisk() error {
	data, err := loadWithChecksum(s.countNetPath(), 12)
	if err != nil {
		return err
	}

	s.countNetAdditions = int(binary.LittleEndian.Uint64(data[0:8]))

	return nil
}

func (s *segment) precomputeCountNetAdditions(updatedCountNetAdditions int) ([]string, error) {
	if s.strategy != segmentindex.StrategyReplace {
		// only "replace" has count net additions, so we are done
		return []string{}, nil
	}

	cnaPath := fmt.Sprintf("%s.tmp", s.countNetPath())
	if err := storeCountNetOnDisk(cnaPath, updatedCountNetAdditions); err != nil {
		return nil, err
	}
	return []string{cnaPath}, nil
}
