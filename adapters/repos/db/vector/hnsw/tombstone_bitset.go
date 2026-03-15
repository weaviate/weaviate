package hnsw

import (
	"sync"
	"sync/atomic"
)

// tombstoneBitset is a concurrent-safe, lock-free reader bitset
// used to store tombstones. Deletions check this bitset in the hot path.
type tombstoneBitset struct {
	bits  atomic.Value // holds []uint64
	Lock  sync.Mutex   // protects writes
	count atomic.Int64
}

func newTombstoneBitset() *tombstoneBitset {
	t := &tombstoneBitset{}
	t.bits.Store(make([]uint64, 0))
	return t
}

func (t *tombstoneBitset) add(id uint64) bool {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	bits := t.bits.Load().([]uint64)
	idx := id / 64
	if idx >= uint64(len(bits)) {
		newBits := make([]uint64, idx+1024) // grow with some extra padding
		copy(newBits, bits)
		bits = newBits
		t.bits.Store(bits)
	}

	val := atomic.LoadUint64(&bits[idx])
	bit := uint64(1) << (id % 64)
	if val&bit == 0 {
		atomic.StoreUint64(&bits[idx], val|bit)
		t.count.Add(1)
		return true
	}
	return false
}

func (t *tombstoneBitset) remove(id uint64) bool {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	bits := t.bits.Load().([]uint64)
	idx := id / 64
	if idx >= uint64(len(bits)) {
		return false
	}

	val := atomic.LoadUint64(&bits[idx])
	bit := uint64(1) << (id % 64)
	if val&bit != 0 {
		atomic.StoreUint64(&bits[idx], val&^bit)
		t.count.Add(-1)
		return true
	}
	return false
}

func (t *tombstoneBitset) has(id uint64) bool {
	bits := t.bits.Load().([]uint64)
	idx := id / 64
	if idx >= uint64(len(bits)) {
		return false
	}
	val := atomic.LoadUint64(&bits[idx])
	return (val & (uint64(1) << (id % 64))) != 0
}

func (t *tombstoneBitset) len() int {
	return int(t.count.Load())
}

func (t *tombstoneBitset) iterate(cb func(id uint64) bool) {
	bits := t.bits.Load().([]uint64)
	for i, val := range bits {
		if val == 0 {
			continue
		}
		v := atomic.LoadUint64(&bits[i])
		for bit := 0; bit < 64; bit++ {
			if v&(uint64(1)<<bit) != 0 {
				if !cb(uint64(i*64 + bit)) {
					return
				}
			}
		}
	}
}
