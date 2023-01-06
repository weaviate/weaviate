package lsmkv

import (
	"fmt"

	"github.com/dgraph-io/sroar"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	return fmt.Errorf("not implemented yet")
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	return fmt.Errorf("not implemented yet")
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	return fmt.Errorf("not implemented yet")
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, sr *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	return fmt.Errorf("not implemented yet")
}

func (b *Bucket) RoaringSetGetBitmap(key []byte, sr *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	return fmt.Errorf("not implemented yet")
}

func checkStrategyRoaringSet(bucketStrat string) error {
	if bucketStrat == StrategyRoaringSet {
		return nil
	}

	return fmt.Errorf("this method requires a roaring set strategy, got: %s",
		bucketStrat)
}
