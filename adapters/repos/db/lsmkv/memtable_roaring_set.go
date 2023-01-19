//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"time"

	"github.com/dgraph-io/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func (m *Memtable) roaringSetAddOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	m.roaringSet.Insert(key, roaringset.Insert{Additions: []uint64{value}})

	m.roaringSetAdjustMeta(1)
	return nil
}

func (m *Memtable) roaringSetRemoveOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	m.roaringSet.Insert(key, roaringset.Insert{Deletions: []uint64{value}})

	m.roaringSetAdjustMeta(1)
	return nil
}

func (m *Memtable) roaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	m.roaringSet.Insert(key, roaringset.Insert{Additions: values})

	m.roaringSetAdjustMeta(len(values))
	return nil
}

func (m *Memtable) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	m.roaringSet.Insert(key, roaringset.Insert{Additions: bm.ToArray()})
	m.roaringSetAdjustMeta(bm.GetCardinality())
	return nil
}

func (m *Memtable) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return roaringset.BitmapLayer{}, err
	}

	s, err := m.roaringSet.Get(key)
	if err != nil {
		return roaringset.BitmapLayer{}, err
	}

	return *s, nil
}

func (m *Memtable) roaringSetAdjustMeta(entriesChanged int) {
	// in the worst case roaring bitmaps take 2 bytes per entry. A reasonable
	// estimation is therefore to take the changed entries and multiply them by
	// 2.
	m.size += uint64(entriesChanged * 2)
	m.metrics.size(m.size)
	m.lastWrite = time.Now()
}
