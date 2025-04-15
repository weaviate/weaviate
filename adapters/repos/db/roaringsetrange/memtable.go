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
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/errors"
)

// As docID (acting as value) can have only single value (acting as key) assigned
// every new value replaces the previous one
// (therefore array data types are not supported)
type Memtable struct {
	logger    logrus.FieldLogger
	additions map[uint64]uint64
	deletions map[uint64]struct{}
}

func NewMemtable(logger logrus.FieldLogger) *Memtable {
	return &Memtable{
		logger:    logger,
		additions: make(map[uint64]uint64),
		deletions: make(map[uint64]struct{}),
	}
}

func (m *Memtable) Insert(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		m.additions[v] = key
	}
}

func (m *Memtable) Delete(key uint64, values []uint64) {
	if len(values) == 0 {
		return
	}

	for _, v := range values {
		delete(m.additions, v)
		m.deletions[v] = struct{}{}
	}
}

func (m *Memtable) Clone() *Memtable {
	clone := &Memtable{logger: m.logger}
	clone.additions = make(map[uint64]uint64, len(m.additions))
	clone.deletions = make(map[uint64]struct{}, len(m.deletions))

	for k := range m.additions {
		clone.additions[k] = m.additions[k]
	}
	for k := range m.deletions {
		clone.deletions[k] = m.deletions[k]
	}

	return clone
}

func (m *Memtable) Nodes() []*MemtableNode {
	if len(m.additions) == 0 && len(m.deletions) == 0 {
		return []*MemtableNode{}
	}

	nnDeletions := sroar.NewBitmap()
	nnAdditions := sroar.NewBitmap()
	var bitsAdditions [64]*sroar.Bitmap

	for v := range m.deletions {
		nnDeletions.Set(v)
	}
	for v := range m.additions {
		nnDeletions.Set(v)
		nnAdditions.Set(v)
	}

	routines := 8
	wg := new(sync.WaitGroup)
	wg.Add(routines - 1)

	for i := 0; i < routines-1; i++ {
		i := i
		errors.GoWrapper(func() {
			for j := 0; j < 64; j += routines {
				bit := i + j
				for value, key := range m.additions {
					if key&(1<<bit) != 0 {
						if bitsAdditions[bit] == nil {
							bitsAdditions[bit] = sroar.NewBitmap()
						}
						bitsAdditions[bit].Set(value)
					}
				}
			}
			wg.Done()
		}, m.logger)
	}

	for bit := routines - 1; bit < 64; bit += routines {
		for value, key := range m.additions {
			if key&(1<<bit) != 0 {
				if bitsAdditions[bit] == nil {
					bitsAdditions[bit] = sroar.NewBitmap()
				}
				bitsAdditions[bit].Set(value)
			}
		}
	}
	wg.Wait()

	nodes := make([]*MemtableNode, 1, 65)
	nodes[0] = &MemtableNode{
		Key:       0,
		Additions: nnAdditions,
		Deletions: nnDeletions,
	}

	for bit := range bitsAdditions {
		if bitsAdditions[bit] != nil {
			nodes = append(nodes, &MemtableNode{
				Key:       uint8(bit) + 1,
				Additions: bitsAdditions[bit],
				Deletions: nil,
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
