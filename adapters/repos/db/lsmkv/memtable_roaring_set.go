package lsmkv

import (
	"github.com/dgraph-io/sroar"
)

func (m *Memtable) roaringSetAddOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	set := newRoaringSet()
	set.additions.Set(value)
	m.roaringSet.insert(key, set)
	return nil
}

func (m *Memtable) roaringSetRemoveOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	set := newRoaringSet()
	set.deletions.Set(value)
	m.roaringSet.insert(key, set)
	return nil
}

func (m *Memtable) roaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	set := newRoaringSet()
	set.additions.SetMany(values)
	m.roaringSet.insert(key, set)
	return nil
}

func (m *Memtable) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return err
	}

	set := newRoaringSet()
	set.additions = bm
	m.roaringSet.insert(key, set)
	return nil
}

func (m *Memtable) roaringSetGet(key []byte) (*roaringSet, error) {
	if err := checkStrategyRoaringSet(m.strategy); err != nil {
		return nil, err
	}

	return m.roaringSet.get(key)
}
