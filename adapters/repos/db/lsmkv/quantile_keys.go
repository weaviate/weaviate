package lsmkv

import (
	"bytes"
	"fmt"
	"sort"
)

func (b *Bucket) QuantileKeys(q int) [][]byte {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	fmt.Printf("before disk.quantileKeys\n")
	keys := b.disk.quantileKeys(q)
	fmt.Printf("after disk.quantileKeys\n")
	return keys
}

func (sg *SegmentGroup) quantileKeys(q int) [][]byte {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	var keys [][]byte

	for _, s := range sg.segments {
		keys = append(keys, s.quantileKeys(q)...)
	}

	// re-sort keys
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// TODO: re-pick keys to make sure they are evenly distributed

	return keys
}

func (s *segment) quantileKeys(q int) [][]byte {
	return s.index.QuantileKeys(q)
}
