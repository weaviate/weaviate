package lsmkv

import (
	"time"

	"github.com/dgraph-io/sroar"
)

func (m *Memtable) roaringSetAddOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	set := newRoaringSet()
	set.additions.Set(value)
	m.roaringSet.insert(key, set)

	m.roaringSetAdjustMeta(1)
	return nil
}

func (m *Memtable) roaringSetRemoveOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	set := newRoaringSet()
	set.deletions.Set(value)
	m.roaringSet.insert(key, set)

	m.roaringSetAdjustMeta(1)
	return nil
}

func (m *Memtable) roaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	set := newRoaringSet()
	set.additions.SetMany(values)
	m.roaringSet.insert(key, set)

	m.roaringSetAdjustMeta(len(values))
	return nil
}

func (m *Memtable) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	// TODO: write into commit log

	set := newRoaringSet()
	set.additions = bm
	m.roaringSet.insert(key, set)

	m.roaringSetAdjustMeta(bm.GetCardinality())
	return nil
}

func (m *Memtable) roaringSetGet(key []byte) (roaringSet, error) {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return roaringSet{}, err
	}

	s, err := m.roaringSet.get(key)
	if err != nil {
		return roaringSet{}, err
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
