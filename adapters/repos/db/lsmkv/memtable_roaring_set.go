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
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func (m *Memtable) roaringSetAddOne(key []byte, value uint64) error {
	return m.roaringSetAddList(key, []uint64{value})
}

func (m *Memtable) roaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(key, roaringset.NewBitmap(values...), roaringset.NewBitmap()); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Additions: values})

	m.roaringSetAdjustMeta(len(values))
	return nil
}

func (m *Memtable) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(key, bm, roaringset.NewBitmap()); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Additions: bm.ToArray()})

	m.roaringSetAdjustMeta(bm.GetCardinality())
	return nil
}

func (m *Memtable) roaringSetRemoveOne(key []byte, value uint64) error {
	return m.roaringSetRemoveList(key, []uint64{value})
}

func (m *Memtable) roaringSetRemoveList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(key, roaringset.NewBitmap(), roaringset.NewBitmap(values...)); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Deletions: values})

	m.roaringSetAdjustMeta(len(values))
	return nil
}

func (m *Memtable) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(key, roaringset.NewBitmap(), bm); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Deletions: bm.ToArray()})

	m.roaringSetAdjustMeta(bm.GetCardinality())
	return nil
}

func (m *Memtable) roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(key, additions, deletions); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{
		Additions: additions.ToArray(),
		Deletions: deletions.ToArray(),
	})

	m.roaringSetAdjustMeta(additions.GetCardinality() + deletions.GetCardinality())
	return nil
}

func (m *Memtable) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return roaringset.BitmapLayer{}, err
	}

	m.RLock()
	defer m.RUnlock()

	return m.roaringSet.Get(key)
}

func (m *Memtable) roaringSetAdjustMeta(entriesChanged int) {
	// in the worst case roaring bitmaps take 2 bytes per entry. A reasonable
	// estimation is therefore to take the changed entries and multiply them by
	// 2.
	m.size += uint64(entriesChanged * 2)
	m.metrics.size(m.size)
	m.lastWrite = time.Now()
}

func (m *Memtable) roaringSetAddCommitLog(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if node, err := roaringset.NewSegmentNode(key, additions, deletions); err != nil {
		return errors.Wrap(err, "create node for commit log")
	} else if err := m.commitlog.add(node); err != nil {
		return errors.Wrap(err, "add node to commit log")
	}
	return nil
}
