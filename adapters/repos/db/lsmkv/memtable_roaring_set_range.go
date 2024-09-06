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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func (m *Memtable) roaringSetRangeAdd(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetRangeAddCommitLog(key, values, nil); err != nil {
		return err
	}

	m.roaringSetRange.Insert(key, values)

	m.roaringSetRangeAdjustMeta(len(values))
	return nil
}

func (m *Memtable) roaringSetRangeRemove(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetRangeAddCommitLog(key, nil, values); err != nil {
		return err
	}

	m.roaringSetRange.Delete(key, values)

	m.roaringSetRangeAdjustMeta(len(values))
	return nil
}

func (m *Memtable) roaringSetRangeAddRemove(key uint64, additions []uint64, deletions []uint64) error {
	if err := CheckStrategyRoaringSetRange(m.strategy); err != nil {
		return err
	}

	m.Lock()
	defer m.Unlock()

	if err := m.roaringSetRangeAddCommitLog(key, additions, deletions); err != nil {
		return err
	}

	m.roaringSetRange.Delete(key, deletions)
	m.roaringSetRange.Insert(key, additions)

	m.roaringSetRangeAdjustMeta(len(additions) + len(deletions))
	return nil
}

func (m *Memtable) roaringSetRangeAdjustMeta(entriesChanged int) {
	// TODO roaring-set-range new estimations

	// in the worst case roaring bitmaps take 2 bytes per entry. A reasonable
	// estimation is therefore to take the changed entries and multiply them by
	// 2.
	m.size += uint64(entriesChanged * 2)
	m.metrics.size(m.size)
	m.updateDirtyAt()
}

func (m *Memtable) roaringSetRangeAddCommitLog(key uint64, additions []uint64, deletions []uint64) error {
	// TODO roaring-set-range improved commit log

	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf, key)
	if node, err := roaringset.NewSegmentNodeList(keyBuf, additions, deletions); err != nil {
		return errors.Wrap(err, "create node for commit log")
	} else if err := m.commitlog.add(node); err != nil {
		return errors.Wrap(err, "add node to commit log")
	}
	return nil
}
