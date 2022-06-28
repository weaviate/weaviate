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
	"time"

	"github.com/pkg/errors"
)

type BucketOption func(b *Bucket) error

func WithStrategy(strategy string) BucketOption {
	return func(b *Bucket) error {
		switch strategy {
		case StrategyReplace, StrategyMapCollection, StrategySetCollection:
		default:
			return errors.Errorf("unrecognized strategy %q", strategy)
		}

		b.strategy = strategy
		return nil
	}
}

func WithMemtableThreshold(threshold uint64) BucketOption {
	return func(b *Bucket) error {
		b.memTableThreshold = threshold
		return nil
	}
}

func WithWalThreshold(threshold uint64) BucketOption {
	return func(b *Bucket) error {
		b.walThreshold = threshold
		return nil
	}
}

func WithIdleThreshold(threshold time.Duration) BucketOption {
	return func(b *Bucket) error {
		b.flushAfterIdle = threshold
		return nil
	}
}

func WithSecondaryIndicies(count uint16) BucketOption {
	return func(b *Bucket) error {
		b.secondaryIndices = count
		return nil
	}
}

func WithLegacyMapSorting() BucketOption {
	return func(b *Bucket) error {
		b.legacyMapSortingBeforeCompaction = true
		return nil
	}
}

type secondaryIndexKeys [][]byte

type SecondaryKeyOption func(s secondaryIndexKeys) error

func WithSecondaryKey(pos int, key []byte) SecondaryKeyOption {
	return func(s secondaryIndexKeys) error {
		if pos > len(s) {
			return errors.Errorf("set secondary index %d on an index of length %d",
				pos, len(s))
		}

		s[pos] = key

		return nil
	}
}

func WithMonitorCount() BucketOption {
	return func(b *Bucket) error {
		if b.strategy != StrategyReplace {
			return errors.Errorf("count monitoring only supported on 'replace' buckets")
		}
		b.monitorCount = true
		return nil
	}
}
