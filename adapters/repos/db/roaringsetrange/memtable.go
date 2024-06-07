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
	bitsAdditions [64]*sroar.Bitmap
	nnAdditions   *sroar.Bitmap
	nnDeletions   *sroar.Bitmap
}

func NewMemtable() *Memtable {
	return &Memtable{
		nnAdditions: sroar.NewBitmap(),
		nnDeletions: sroar.NewBitmap(),
	}
}

func (m *Memtable) Insert(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.nnAdditions.Set(v)
		m.nnDeletions.Set(v)
	}

	for bit := range m.bitsAdditions {
		if key&(1<<bit) == 0 {
			if m.bitsAdditions[bit] != nil {
				for _, v := range values {
					m.bitsAdditions[bit].Remove(v)
				}
			}
		} else {
			if m.bitsAdditions[bit] == nil {
				m.bitsAdditions[bit] = sroar.NewBitmap()
			}
			for _, v := range values {
				m.bitsAdditions[bit].Set(v)
			}
		}
	}
}

func (m *Memtable) Delete(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.nnDeletions.Set(v)
		m.nnAdditions.Remove(v)
	}

	for bit := range m.bitsAdditions {
		if m.bitsAdditions[bit] != nil {
			for _, v := range values {
				m.bitsAdditions[bit].Remove(v)
			}
		}
	}
}

func (m *Memtable) Nodes() []*MemtableNode {
	if m.nnAdditions.IsEmpty() && m.nnDeletions.IsEmpty() {
		return []*MemtableNode{}
	}

	nodes := make([]*MemtableNode, 1, 65)
	nodes[0] = &MemtableNode{
		Key:       0,
		Additions: roaringset.Condense(m.nnAdditions),
		Deletions: roaringset.Condense(m.nnDeletions),
	}

	bmEmpty := sroar.NewBitmap()

	for bit := range m.bitsAdditions {
		if m.bitsAdditions[bit] != nil && !m.bitsAdditions[bit].IsEmpty() {
			nodes = append(nodes, &MemtableNode{
				Key:       uint8(bit) + 1,
				Additions: roaringset.Condense(m.bitsAdditions[bit]),
				Deletions: bmEmpty,
			})
		}
	}

	return nodes
}

type MemtableNode struct {
	Key       uint8
	Additions *sroar.Bitmap
	Deletions *sroar.Bitmap
}
