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

package roaringsetrange

import (
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type Memtable struct {
	bitsAdditions map[uint8]*sroar.Bitmap
	nnAdditions   *sroar.Bitmap
	nnDeletions   *sroar.Bitmap
}

func NewMemtable() *Memtable {
	return &Memtable{
		bitsAdditions: make(map[uint8]*sroar.Bitmap),
		nnAdditions:   sroar.NewBitmap(),
		nnDeletions:   sroar.NewBitmap(),
	}
}

func (m *Memtable) Insert(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	bmValues := roaringset.NewBitmap(values...)
	m.nnDeletions.Or(bmValues)
	m.nnAdditions.Or(bmValues)

	for i := uint8(0); i < 64; i++ {
		bitAdditions, ok := m.bitsAdditions[i]

		if key&(1<<i) == 0 {
			if ok {
				bitAdditions.AndNot(bmValues)
			}
		} else {
			if ok {
				bitAdditions.Or(bmValues)
			} else {
				m.bitsAdditions[i] = bmValues.Clone()
			}
		}
	}
}

func (m *Memtable) Delete(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	bmValues := roaringset.NewBitmap(values...)
	m.nnDeletions.Or(bmValues)

	bmValues.And(m.nnAdditions)
	if bmValues.IsEmpty() {
		return
	}

	m.nnAdditions.AndNot(bmValues)
	for _, bitAdditions := range m.bitsAdditions {
		bitAdditions.AndNot(bmValues)
	}
}

func (m *Memtable) Nodes() []*MemtableNode {
	if m.nnAdditions.IsEmpty() && m.nnDeletions.IsEmpty() {
		return []*MemtableNode{}
	}

	nodes := make([]*MemtableNode, 1, 1+len(m.bitsAdditions))
	nodes[0] = &MemtableNode{
		Key:       0,
		Additions: roaringset.Condense(m.nnAdditions),
		Deletions: roaringset.Condense(m.nnDeletions),
	}

	bmEmpty := sroar.NewBitmap()
	l := 1
	for i := uint8(0); i < 64; i++ {
		if bitAdditions, ok := m.bitsAdditions[i]; ok && !bitAdditions.IsEmpty() {
			l++
			nodes = append(nodes, &MemtableNode{
				Key:       i + 1,
				Additions: roaringset.Condense(bitAdditions),
				Deletions: bmEmpty,
			})
		}
	}

	return nodes[:l]
}

type MemtableNode struct {
	Key       uint8
	Additions *sroar.Bitmap
	Deletions *sroar.Bitmap
}
