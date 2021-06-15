package lsmkv

import "github.com/pkg/errors"

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

func WithSecondaryIndicies(count uint16) BucketOption {
	return func(b *Bucket) error {
		b.secondaryIndices = count
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
