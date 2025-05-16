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
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/theOneTrueFileStore"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}



	set, _ := b.RoaringSetGet(key)
	if set == nil {
		return nil
	}

	bm := sroar.FromSortedList([]uint64{value})
	set.Or(bm)
	out := set.ToBuffer()


	theOneTrueFileStore.TheOneTrueFileStore().Put(b.KeyPath(key), out)
	return nil
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	

	set, _ := b.RoaringSetGet(key)
	if set == nil {
		return nil
	}

	bm := sroar.FromSortedList([]uint64{value})
	set.AndNot(bm)
	out := set.ToBuffer()

	theOneTrueFileStore.TheOneTrueFileStore().Put(b.KeyPath(key), out)
	return nil
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	

	set, err := b.RoaringSetGet(key)
	if set == nil {
		return nil
	}


	bm := sroar.FromSortedList(values)

	set.Or(bm)
	if err != nil {
		return err
	}

	out := set.ToBuffer()
	theOneTrueFileStore.TheOneTrueFileStore().Put(b.KeyPath(key), out)
	return nil
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	

	set, _ := b.RoaringSetGet(key)
	if set == nil {
		return nil
	}


	set.Or(bm)

	out := set.ToBuffer()

	theOneTrueFileStore.TheOneTrueFileStore().Put(b.KeyPath(key), out)
	return nil
}

func (b *Bucket) RoaringSetGet(key []byte) (*sroar.Bitmap, error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, err
	}

	

	data, err := theOneTrueFileStore.TheOneTrueFileStore().Get(b.KeyPath(key))
	if err != nil {
		return sroar.NewBitmap(), nil
	}

	out := sroar.FromBufferWithCopy(data)
	return out, nil

}
