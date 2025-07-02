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
	"encoding/binary"
	"errors"

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
	return s.buildPath("%s.cna")
}

func (s *segment) initCNAFromData(meta metadata) error {
	if !s.calcCountNetAdditions || s.strategy != segmentindex.StrategyReplace {
		return nil
	}

	s.countNetAdditions = int(binary.LittleEndian.Uint64(meta.NetAdditions[0:8]))

	return nil
}

func (s *segment) recalcCountNetAdditions(exists existsOnLowerSegmentsFn, precomputedCNAValue *int) ([]byte, error) {
	if !s.calcCountNetAdditions || s.strategy != segmentindex.StrategyReplace {
		return nil, nil
	}

	if precomputedCNAValue != nil {
		s.countNetAdditions = *precomputedCNAValue
	} else {
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

		if lastErr != nil {
			return nil, lastErr
		}

		s.countNetAdditions = countNet
	}

	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(s.countNetAdditions))
	return data, nil
}
