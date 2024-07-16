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

package iterableconnections

import (
	"encoding/binary"
	"sort"
)

type IterableConnections interface {
	Next() (uint64, bool)
	Reset()
	Copy() IterableConnections
}

type arrIterableConnections struct {
	index    int
	elements []uint64
}

func NewArrIterableConnections(connections []uint64) IterableConnections {
	connectionsReusable := make([]uint64, len(connections))
	copy(connectionsReusable, connections)
	return &arrIterableConnections{
		index:    0,
		elements: connectionsReusable,
	}
}

func (ic *arrIterableConnections) Copy() IterableConnections {
	return ic
}

func (ic *arrIterableConnections) Next() (uint64, bool) {
	if ic.index == len(ic.elements) {
		return 0, false
	}
	x := ic.elements[ic.index]
	ic.index++
	return x, true
}

func (ic *arrIterableConnections) Reset() {
	ic.index = 0
}

type varintIterableConnections struct {
	index    int
	elements []byte
	acc      uint64
}

func NewVarintIterableConnections(connections []uint64) IterableConnections {
	sort.Slice(connections, func(i, j int) bool {
		return connections[i] < connections[j]
	})
	acc := uint64(0)
	connectionsReusable := make([]byte, len(connections)*9)
	offset := 0
	for _, x := range connections {
		offset += binary.PutUvarint(connectionsReusable[offset:], x-acc)
		acc = x
	}
	finalArr := make([]byte, offset)
	copy(finalArr, connectionsReusable[:offset])
	return &varintIterableConnections{
		index:    0,
		elements: finalArr,
		acc:      0,
	}
}

func (ic *varintIterableConnections) Copy() IterableConnections {
	return &varintIterableConnections{
		index:    0,
		elements: ic.elements,
		acc:      0,
	}
}

func (ic *varintIterableConnections) Next() (uint64, bool) {
	if ic.index == len(ic.elements) {
		return 0, false
	}
	x, len := binary.Uvarint(ic.elements[ic.index:])
	ic.index += len
	ic.acc += uint64(x)
	return ic.acc, true
}

func (ic *varintIterableConnections) Reset() {
	ic.index = 0
	ic.acc = 0
}
