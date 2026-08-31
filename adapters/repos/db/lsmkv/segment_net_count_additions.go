//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// ErrInvalidChecksum indicates that the read file should not be trusted. For
// any pre-computed data this is a recoverable issue, as the data can simply be
// re-computed at read-time.
var ErrInvalidChecksum = errors.New("invalid checksum")

// canRecomputeSidecar reports whether a failed load of a bloom filter, count net
// additions or metadata file can be resolved by computing it again: the file is
// corrupt, or it is gone even though the file list said it exists.
func canRecomputeSidecar(err error) bool {
	return errors.Is(err, ErrInvalidChecksum) || errors.Is(err, fs.ErrNotExist)
}

const CountNetAdditionsFileSuffix = ".cna"

// existOnLowerSegments is a simple function that can be passed at segment
// initialization time to check if any of the keys are truly new or previously
// seen. This can in turn be used to build up the net count additions. The
// reason this is abstract:
type existsOnLowerSegmentsFn func(key []byte) (bool, error)

func (s *segment) countNetPath() string {
	return s.buildPath("%s.cna")
}

// errApproximateNetAdditions reports a count that could not consult every lower
// segment. The count is still usable for the life of the process, but its
// callers must not write it to a sidecar: nothing recomputes a sidecar that
// parses, so the wrong number would outlive the segment that caused it.
var errApproximateNetAdditions = errors.New("net additions count is approximate")

// computeNetAdditions walks this segment's keys and nets the new ones against
// its tombstones, asking exists whether each key is already held further down.
// A key exists cannot answer for is left out of the total rather than assumed
// new, which would inflate the count in one direction only.
func (s *segment) computeNetAdditions(exists existsOnLowerSegmentsFn) (int, error) {
	var lookupErr error
	unanswered := 0
	countNet := 0
	cb := func(key []byte, tombstone bool) {
		existedOnPrior, err := exists(key)
		if err != nil {
			lookupErr = err
			unanswered++
			return
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

	if lookupErr != nil {
		s.logger.WithField("path", s.path).
			Errorf("object count omits %d keys whose lower segments could not be read: %v",
				unanswered, lookupErr)
		return countNet, fmt.Errorf("%w: %w", errApproximateNetAdditions, lookupErr)
	}
	return countNet, nil
}

func (s *segment) initCountNetAdditions(exists existsOnLowerSegmentsFn, overwrite bool, precomputedCNAValue *int, existingFilesList map[string]int64) error {
	if s.strategy != segmentindex.StrategyReplace {
		// replace is the only strategy that supports counting
		return nil
	}

	path := s.countNetPath()
	s.metaPaths = append(s.metaPaths, path)

	loadFromDisk, err := fileExistsInList(existingFilesList, filepath.Base(path))
	if err != nil {
		return err
	}
	if loadFromDisk {
		if overwrite {
			err := os.Remove(path)
			if err != nil {
				return fmt.Errorf("delete existing net additions counter %s: %w", path, err)
			}
		} else {
			err = s.loadCountNetFromDisk()
			if err == nil {
				return nil
			}

			if !canRecomputeSidecar(err) {
				return err
			}

			// now continue re-calculating
		}
	}

	if precomputedCNAValue != nil {
		s.countNetAdditions = *precomputedCNAValue
	} else {
		count, err := s.computeNetAdditions(exists)
		s.countNetAdditions = count
		if errors.Is(err, errApproximateNetAdditions) {
			// leaving the sidecar absent is what makes the next load recompute,
			// so the count corrects itself once the segment below can be read
			return nil
		}
	}

	if err := s.storeCountNetOnDisk(); err != nil {
		return fmt.Errorf("store count net additions on disk: %w", err)
	}

	return nil
}

func (s *segment) storeCountNetOnDisk() error {
	return storeCountNetOnDisk(s.countNetPath(), s.countNetAdditions, s.observeMetaWrite)
}

func storeCountNetOnDisk(path string, value int, observeWrite diskio.MeteredWriterCallback) error {
	rw := byteops.NewReadWriter(make([]byte, byteops.Uint64Len+byteops.Uint32Len))
	rw.MoveBufferPositionForward(byteops.Uint32Len) // leave space for checksum
	rw.WriteUint64(uint64(value))

	return writeWithChecksum(rw, path, observeWrite)
}

func (s *segment) loadCountNetFromDisk() error {
	data, err := loadWithChecksum(s.countNetPath(), 12, s.metrics.ReadObserver("netAdditions"))
	if err != nil {
		return err
	}

	s.countNetAdditions = int(binary.LittleEndian.Uint64(data[0:8]))

	return nil
}

// ReadCountNetAdditionsFile reads a .cna file and returns the count net additions value
// Returns (count, nil) if successful, (0, error) if the file is invalid or corrupted
func ReadCountNetAdditionsFile(path string) (int64, error) {
	data, err := loadWithChecksum(path, 12, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to read .cna file: %w", err)
	}

	// Extract count value (first 8 bytes, uint64 little-endian)
	count := int64(binary.LittleEndian.Uint64(data[0:8]))

	return count, nil
}
