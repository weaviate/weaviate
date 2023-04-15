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

func (l *Memtable) flush() error {
	// close the commit log first, this also forces it to be fsynced. If
	// something fails there, don't proceed with flushing. The commit log will
	// only be deleted at the very end, if the flush was successful
	// (indicated by a successful close of the flush file - which indicates a
	// successful fsync)

	if err := l.commitlog.close(); err != nil {
		return errors.Wrap(err, "close commit log file")
	}

	if l.Size() == 0 {
		// this is an empty memtable, nothing to do
		// however, we still have to cleanup the commit log, otherwise we will
		// attempt to recover from it on the next cycle
		if err := l.commitlog.delete(); err != nil {
			return errors.Wrap(err, "delete commit log file")
		}
		return nil
	}

	f, err := os.Create(l.path + ".db")
	if err != nil {
		return err
	}

	w := bufio.NewWriterSize(f, int(float64(l.size)*1.3)) // calculate 30% overhead for disk representation

	var keys []segmentindex.Key
	switch l.strategy {
	case StrategyReplace:
		if keys, err = l.flushDataReplace(w); err != nil {
			return err
		}

	case StrategySetCollection:
		if keys, err = l.flushDataSet(w); err != nil {
			return err
		}

	case StrategyRoaringSet:
		if keys, err = l.flushDataRoaringSet(w); err != nil {
			return err
		}

	case StrategyMapCollection:
		if keys, err = l.flushDataMap(w); err != nil {
			return err
		}

	default:
		return fmt.Errorf("cannot flush strategy %s", l.strategy)
	}

	indices := &segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: l.secondaryIndices,
		ScratchSpacePath:    l.path + ".scratch.d",
	}

	if _, err := indices.WriteTo(w); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	// only now that the file has been flushed is it safe to delete the commit log
	// TODO: there might be an interest in keeping the commit logs around for
	// longer as they might come in handy for replication
	return l.commitlog.delete()
}

func (l *Memtable) flushDataReplace(f io.Writer) ([]segmentindex.Key, error) {
	flat := l.key.flattenInOrder()

	totalDataLength := totalKeyAndValueSize(flat)
	perObjectAdditions := len(flat) * (1 + 8 + 4 + int(l.secondaryIndices)*4) // 1 byte for the tombstone, 8 bytes value length encoding, 4 bytes key length encoding, + 4 bytes key encoding for every secondary index
	headerSize := segmentindex.HeaderSize
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + perObjectAdditions + headerSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: l.secondaryIndices,
		Strategy:         SegmentStrategyFromString(l.strategy),
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
			secondaryIndexCount: l.secondaryIndices,
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

func (l *Memtable) flushDataSet(f io.Writer) ([]segmentindex.Key, error) {
	flat := l.keyMulti.flattenInOrder()
	return l.flushDataCollection(f, flat)
}

func (l *Memtable) flushDataMap(f io.Writer) ([]segmentindex.Key, error) {
	l.RLock()
	flat := l.keyMap.flattenInOrder()
	l.RUnlock()

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
	return l.flushDataCollection(f, asMulti)
}

func (l *Memtable) flushDataCollection(f io.Writer,
	flat []*binarySearchNodeMulti,
) ([]segmentindex.Key, error) {
	totalDataLength := totalValueSizeCollection(flat)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0, // always level zero on a new one
		Version:          0, // always version 0 for now
		SecondaryIndices: l.secondaryIndices,
		Strategy:         SegmentStrategyFromString(l.strategy),
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
