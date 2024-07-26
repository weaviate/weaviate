package lsmkv

import (
	"bytes"
	"sort"
)

func (b *Bucket) QuantileKeys(q int) [][]byte {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	keys := b.disk.quantileKeys(q)
	return keys
}

func (sg *SegmentGroup) quantileKeys(q int) [][]byte {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	var keys [][]byte

	if len(sg.segments) == 0 {
		return keys
	}

	for _, s := range sg.segments {
		keys = append(keys, s.quantileKeys(q)...)
	}

	// re-sort keys
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	finalKeys := make([][]byte, q)
	for i := range finalKeys {
		finalKeys[i] = keys[len(keys)/q*i]
	}

	return finalKeys
}

func (s *segment) quantileKeys(q int) [][]byte {
	return s.index.QuantileKeys(q)
}
