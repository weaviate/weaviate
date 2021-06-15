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
