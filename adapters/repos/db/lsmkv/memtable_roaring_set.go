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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func (m *Memtable) roaringSetAddOne(key []byte, value uint64) error {
	return m.roaringSetAddList(key, []uint64{value})
}

func (m *Memtable) roaringSetAddList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	node, err := roaringset.NewSegmentNodeList(key, values, []uint64{})
	if err != nil {
		return fmt.Errorf("create node for commit log: %w", err)
	}
	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(node); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Additions: values})

	m.roaringSetAdjustMeta(len(values))
	return nil
}

// roaringSetAddBatch writes multiple key-values pairs under a single
// memtable lock acquisition. Commit log nodes are pre-allocated outside the
// lock to keep the critical section as short as possible.
func (m *Memtable) roaringSetAddBatch(entries []RoaringSetBatchEntry) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// Pre-allocate all commit log nodes outside the lock.
	nodes := make([]*roaringset.SegmentNodeList, len(entries))
	totalValues := 0
	for i, e := range entries {
		node, err := roaringset.NewSegmentNodeList(e.Key, e.Values, []uint64{})
		if err != nil {
			return fmt.Errorf("create node for commit log: %w", err)
		}
		nodes[i] = node
		totalValues += len(e.Values)
	}

	m.Lock()
	defer m.Unlock()

	for i, node := range nodes {
		if err := m.roaringSetAddCommitLog(node); err != nil {
			return err
		}
		m.roaringSet.Insert(entries[i].Key, roaringset.Insert{Additions: entries[i].Values})
	}

	m.roaringSetAdjustMeta(totalValues)
	return nil
}

func (m *Memtable) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	array := bm.ToArray()
	node, err := roaringset.NewSegmentNodeList(key, array, []uint64{})
	if err != nil {
		return fmt.Errorf("create node for commit log: %w", err)
	}
	cardinality := bm.GetCardinality()

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(node); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Additions: array})

	m.roaringSetAdjustMeta(cardinality)
	return nil
}

func (m *Memtable) roaringSetRemoveOne(key []byte, value uint64) error {
	return m.roaringSetRemoveList(key, []uint64{value})
}

func (m *Memtable) roaringSetRemoveList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	node, err := roaringset.NewSegmentNodeList(key, []uint64{}, values)
	if err != nil {
		return fmt.Errorf("create node for commit log: %w", err)
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(node); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Deletions: values})

	m.roaringSetAdjustMeta(len(values))
	return nil
}

// roaringSetRemoveBatch removes multiple key-values pairs under a single
// memtable lock acquisition. Commit log nodes are pre-allocated outside the
// lock to keep the critical section as short as possible.
func (m *Memtable) roaringSetRemoveBatch(entries []RoaringSetBatchEntry) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// Pre-allocate all commit log nodes outside the lock.
	nodes := make([]*roaringset.SegmentNodeList, len(entries))
	totalValues := 0
	for i, e := range entries {
		node, err := roaringset.NewSegmentNodeList(e.Key, []uint64{}, e.Values)
		if err != nil {
			return fmt.Errorf("create node for commit log: %w", err)
		}
		nodes[i] = node
		totalValues += len(e.Values)
	}

	m.Lock()
	defer m.Unlock()

	for i, node := range nodes {
		if err := m.roaringSetAddCommitLog(node); err != nil {
			return err
		}
		m.roaringSet.Insert(entries[i].Key, roaringset.Insert{Deletions: entries[i].Values})
	}

	m.roaringSetAdjustMeta(totalValues)
	return nil
}

func (m *Memtable) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	array := bm.ToArray()
	node, err := roaringset.NewSegmentNodeList(key, []uint64{}, array)
	if err != nil {
		return fmt.Errorf("create node for commit log: %w", err)
	}
	cardinality := bm.GetCardinality()
	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(node); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{Deletions: array})

	m.roaringSetAdjustMeta(cardinality)
	return nil
}

func (m *Memtable) roaringSetAddRemoveSlices(key []byte, additions []uint64, deletions []uint64) error {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	node, err := roaringset.NewSegmentNodeList(key, additions, deletions)
	if err != nil {
		return fmt.Errorf("create node for commit log: %w", err)
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetAddCommitLog(node); err != nil {
		return err
	}

	m.roaringSet.Insert(key, roaringset.Insert{
		Additions: additions,
		Deletions: deletions,
	})

	m.roaringSetAdjustMeta(len(additions) + len(deletions))
	return nil
}

// returned bitmaps are cloned and safe to mutate
func (m *Memtable) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if err := CheckStrategyRoaringSet(m.strategy); err != nil {
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
	m.metrics.observeSize(m.size)
	m.updateDirtyAt()
}

func (m *Memtable) roaringSetAddCommitLog(node *roaringset.SegmentNodeList) error {
	if err := m.commitlog.add(node); err != nil {
		return errors.Wrap(err, "add node to commit log")
	}
	return nil
}
