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

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func (m *MemtableSingle) roaringSetAddOne(key []byte, value uint64) error {
	return m.roaringSetAddList(key, []uint64{value})
}

func (m *MemtableSingle) roaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	additions := roaringset.NewBitmap(values...)
	deletions := roaringset.NewBitmap()
	if err := m.roaringSetAddCommitLog(key, additions, deletions); err != nil {
		return err
	}

	m.roaringSet.InsertBitmaps(key, roaringset.Insert{Additions: values}, additions, deletions)

	m.roaringSetAdjustMeta(len(values))
	return nil
}

// KeyValue struct
type KeyValue struct {
	Key    []byte
	Values []uint64
}

func (m *MemtableSingle) roaringSetAddListBatch(batch []KeyValue) []error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return []error{err}
	}

	// empty error slice
	errs := []error{}

	totalValueCount := 0

	m.Lock()
	defer m.Unlock()

	for _, kv := range batch {
		key := kv.Key
		values := kv.Values
		additions := roaringset.NewBitmap(values...)
		deletions := roaringset.NewBitmap()
		err := m.roaringSetAddCommitLog(key, additions, deletions)
		if err != nil {
			errs = append(errs, err)
		}
		m.roaringSet.InsertBitmaps(key, roaringset.Insert{Additions: values}, additions, deletions)
		totalValueCount += len(values)
	}

	m.roaringSetAdjustMeta(totalValueCount)
	return errs
}

func (m *MemtableSingle) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
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

func (m *MemtableSingle) roaringSetRemoveOne(key []byte, value uint64) error {
	return m.roaringSetRemoveList(key, []uint64{value})
}

func (m *MemtableSingle) roaringSetRemoveList(key []byte, values []uint64) error {
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

func (m *MemtableSingle) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
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

func (m *MemtableSingle) roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
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

func (m *MemtableSingle) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return roaringset.BitmapLayer{}, err
	}

	m.RLock()
	defer m.RUnlock()

	return m.roaringSet.Get(key)
}

func (m *MemtableSingle) roaringSetAdjustMeta(entriesChanged int) {
	// in the worst case roaring bitmaps take 2 bytes per entry. A reasonable
	// estimation is therefore to take the changed entries and multiply them by
	// 2.
	m.size += uint64(entriesChanged * 2)
	m.metrics.size(m.size)
	m.lastWrite = time.Now()
}

func (m *MemtableSingle) roaringSetAddCommitLog(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if node, err := roaringset.NewSegmentNode(key, additions, deletions); err != nil {
		return errors.Wrap(err, "create node for commit log")
	} else if err := m.commitlog.add(node); err != nil {
		return errors.Wrap(err, "add node to commit log")
	}
	return nil
}

/*
func (m *MemtableSingle) roaringSetAddCommitLogBatch(keys []byte, additionss []*sroar.Bitmap, deletionss []*sroar.Bitmap) error {
	if node, err := roaringset.NewSegmentNode(key, additions, deletions); err != nil {
		return errors.Wrap(err, "create node for commit log")
	} else if err := m.commitlog.addBatch(node); err != nil {
		return errors.Wrap(err, "add node to commit log")
	}
	return nil
}
*/

func (m *MemtableSingle) flattenNodesRoaringSet() []*roaringset.BinarySearchNode {
	return m.roaringSet.FlattenInOrder()
}
