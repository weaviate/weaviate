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

package segmentindex

import (
	"bytes"
	"io"
	"sort"

	"github.com/weaviate/weaviate/usecases/byteops"
)

type Indexes struct {
	Keys                []Key
	SecondaryIndexCount uint16
	ScratchSpacePath    string
}

func (s *Indexes) WriteTo(w io.Writer) (int64, error) {
	var currentOffset uint64 = HeaderSize
	if len(s.Keys) > 0 {
		currentOffset = uint64(s.Keys[len(s.Keys)-1].ValueEnd)
	}
	currentOffset += uint64(s.SecondaryIndexCount) * 8

	primaryIndex := s.buildPrimary(s.Keys)
	currentOffset += uint64(primaryIndex.Size())

	offsetSecondaryStart := currentOffset
	var secondaryTrees []*Tree
	if s.SecondaryIndexCount > 0 {
		secondaryTrees = make([]*Tree, s.SecondaryIndexCount)
		for pos := range secondaryTrees {
			secondary, err := s.buildSecondary(s.Keys, pos)
			if err != nil {
				return 0, err
			}
			secondaryTrees[pos] = &secondary
			currentOffset += uint64(secondary.Size())
		}
	}

	buf := make([]byte, currentOffset)
	rw := byteops.NewReadWriter(buf)

	for _, secondary := range secondaryTrees {
		rw.WriteUint64(offsetSecondaryStart)
		offsetSecondaryStart += uint64(secondary.Size())
	}

	_, err := primaryIndex.MarshalBinaryInto(&rw)
	if err != nil {
		return 0, err
	}

	for _, secondary := range secondaryTrees {
		_, err := secondary.MarshalBinaryInto(&rw)
		if err != nil {
			return 0, err
		}
	}
	written, err := w.Write(rw.Buffer)
	if err != nil {
		return 0, err
	}

	return int64(written), nil
}

func (s *Indexes) buildSecondary(keys []Key, pos int) (Tree, error) {
	keyNodes := make([]Node, len(keys))
	i := 0
	for _, key := range keys {
		if pos >= len(key.SecondaryKeys) {
			// a secondary key is not guaranteed to be present. For example, a delete
			// operation could pe performed using only the primary key
			continue
		}

		keyNodes[i] = Node{
			Key:   key.SecondaryKeys[pos],
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
		i++
	}

	keyNodes = keyNodes[:i]

	sort.Slice(keyNodes, func(a, b int) bool {
		return bytes.Compare(keyNodes[a].Key, keyNodes[b].Key) < 0
	})

	index := NewBalanced(keyNodes)
	return index, nil
}

// assumes sorted keys and does NOT sort them again
func (s *Indexes) buildPrimary(keys []Key) Tree {
	keyNodes := make([]Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = Node{
			Key:   key.Key,
			Start: uint64(key.ValueStart),
			End:   uint64(key.ValueEnd),
		}
	}
	index := NewBalanced(keyNodes)

	return index
}
