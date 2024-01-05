//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func preflush(m Memtable) (*os.File, *bufio.Writer, error) {
	// close the commit log first, this also forces it to be fsynced. If
	// something fails there, don't proceed with flushing. The commit log will
	// only be deleted at the very end, if the flush was successful
	// (indicated by a successful close of the flush file - which indicates a
	// successful fsync)

	if err := m.CommitlogClose(); err != nil {
		return nil, nil, errors.Wrap(err, "close commit log file")
	}

	if m.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := m.CommitlogDelete(); err != nil {
			return nil, nil, errors.Wrap(err, "delete commit log file")
		}
		return nil, nil, nil
	}

	f, err := os.Create(m.Path() + ".db")
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriterSize(f, int(float64(m.Size())*1.3)) // calculate 30% overhead for disk representation

	return f, w, err
}

func postflush(m Memtable, f *os.File, w *bufio.Writer, keys []segmentindex.Key) error {
	indices := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: m.SecondaryIndices(),
		ScratchSpacePath:    m.Path() + ".scratch.d",
	}

	if _, err := indices.WriteTo(w); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return errors.Wrap(err, "flush")
	}

	if err := f.Close(); err != nil {
		return errors.Wrap(err, "close")
	}

	if err := m.CommitlogDelete(); err != nil {
		return errors.Wrap(err, "delete commit log file")
	}

	return nil
}

func (m *MemtableSingle) flush() error {
	return flush(m)
}

func flush(m Memtable) error {
	f, w, err := preflush(m)
	if err != nil {
		return err
	}

	if f == nil {
		return nil
	}
	var keys []segmentindex.Key
	switch m.Strategy() {
	case StrategyReplace:
		flat := m.flattenInOrderKey()
		if keys, err = flushDataReplace(m, flat, w); err != nil {
			return err
		}

	case StrategySetCollection:
		flat := m.flattenInOrderKeyMulti()
		if keys, err = flushDataCollection(m, flat, w); err != nil {
			return err
		}

	case StrategyRoaringSet:
		flat := m.flattenNodesRoaringSet()
		if keys, err = flushDataRoaringSet(m, flat, w); err != nil {
			return err
		}

	case StrategyMapCollection:
		flat := m.flattenInOrderKeyMap()
		if keys, err = flushDataMap(m, flat, w); err != nil {
			return err
		}

	default:
		return fmt.Errorf("cannot flush strategy %s", m.Strategy())
	}

	return postflush(m, f, w, keys)
}

func flushDataReplace(m Memtable, flat []*binarySearchNode, f io.Writer) ([]segmentindex.Key, error) {
	totalDataLength := totalKeyAndValueSize(flat)
	perObjectAdditions := len(flat) * (1 + 8 + 4 + int(m.SecondaryIndices())*4) // 1 byte for the tombstone, 8 bytes value length encoding, 4 bytes key length encoding, + 4 bytes key encoding for every secondary index
	headerSize := segmentindex.HeaderSize
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + perObjectAdditions + headerSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: m.SecondaryIndices(),
		Strategy:         SegmentStrategyFromString(m.Strategy()),
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, err
	}
	headerSize = int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		segNode := &segmentReplaceNode{
			offset:              totalWritten,
			tombstone:           node.tombstone,
			value:               node.value,
			primaryKey:          node.key,
			secondaryKeys:       node.secondaryKeys,
			secondaryIndexCount: m.SecondaryIndices(),
		}

		ki, err := segNode.KeyIndexAndWriteTo(f)
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func flushDataMap(m Memtable, flat []*binarySearchNodeMap, f io.Writer) ([]segmentindex.Key, error) {
	// by encoding each map pair we can force the same structure as for a
	// collection, which means we can reuse the same flushing logic
	asMulti := make([]*binarySearchNodeMulti, len(flat))
	for i, mapNode := range flat {
		asMulti[i] = &binarySearchNodeMulti{
			key:    mapNode.key,
			values: make([]value, len(mapNode.values)),
		}

		for j := range asMulti[i].values {
			enc, err := mapNode.values[j].Bytes()
			if err != nil {
				return nil, err
			}

			asMulti[i].values[j] = value{
				value:     enc,
				tombstone: mapNode.values[j].Tombstone,
			}
		}

	}
	return flushDataCollection(m, asMulti, f)
}

func flushDataCollection(m Memtable,
	flat []*binarySearchNodeMulti,
	f io.Writer,
) ([]segmentindex.Key, error) {
	totalDataLength := totalValueSizeCollection(flat)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: m.SecondaryIndices(),
		Strategy:         SegmentStrategyFromString(m.Strategy()),
	}

	n, err := header.WriteTo(f)
	if err != nil {
		return nil, err
	}
	headerSize := int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		ki, err := (&segmentCollectionNode{
			values:     node.values,
			primaryKey: node.key,
			offset:     totalWritten,
		}).KeyIndexAndWriteTo(f)
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}

	return keys, nil
}

func totalKeyAndValueSize(in []*binarySearchNode) int {
	var sum int
	for _, n := range in {
		sum += len(n.value)
		sum += len(n.key)
		for _, sec := range n.secondaryKeys {
			sum += len(sec)
		}
	}

	return sum
}

func totalValueSizeCollection(in []*binarySearchNodeMulti) int {
	var sum int
	for _, n := range in {
		sum += 8 // uint64 to indicate array length
		for _, v := range n.values {
			sum += 1 // bool to indicate value tombstone
			sum += 8 // uint64 to indicate value length
			sum += len(v.value)
		}

		sum += 4 // uint32 to indicate key size
		sum += len(n.key)
	}

	return sum
}
