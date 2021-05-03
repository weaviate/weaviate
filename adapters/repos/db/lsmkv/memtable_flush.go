package lsmkv

import (
	"encoding/binary"
	"math/rand"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func (l *Memtable) flush() error {
	// close the commit log first, this also forces it to be fsynced. If
	// something fails there, don't proceed with flushing. The commit log will
	// not only be deleted at the very end, if the flush was successful
	// (indicated by a successful close of the flush file - which indicates a
	// successful fsync)
	if err := l.commitlog.close(); err != nil {
		return errors.Wrap(err, "close commit log file")
	}

	f, err := os.Create(l.path + ".db")
	if err != nil {
		return err
	}

	var keys []keyIndex
	switch l.strategy {
	case StrategyReplace:
		if keys, err = l.flushDataReplace(f); err != nil {
			return err
		}

	case StrategySetCollection, StrategyMapCollection:
		if keys, err = l.flushDataCollection(f); err != nil {
			return err
		}

	}

	// shuffle keys so we don't end up with an unbalanced binary tree, as we
	// would if they were perfectly ordered
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	index := segmentindex.NewTree(len(keys))
	for _, key := range keys {
		index.Insert(key.key, uint64(key.valueStart), uint64(key.valueEnd))
	}

	indexBytes, err := index.MarshalBinary()
	if err != nil {
		return err
	}

	if _, err := f.Write(indexBytes); err != nil {
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

func (l *Memtable) flushDataReplace(f *os.File) ([]keyIndex, error) {
	flat := l.key.flattenInOrder()

	totalDataLength := totalValueSize(flat)
	perObjectAdditions := len(flat) * 1 // 1 byte for the tombstone
	offset := 12                        // 2 bytes for level, 2 bytes for strategy, 8 bytes for this indicator itself
	indexPos := uint64(totalDataLength + perObjectAdditions + offset)
	level := uint16(0) // always level zero on a new one

	if err := binary.Write(f, binary.LittleEndian, &level); err != nil {
		return nil, err
	}
	if err := binary.Write(f, binary.LittleEndian, SegmentStrategyReplace); err != nil {
		return nil, err
	}
	if err := binary.Write(f, binary.LittleEndian, &indexPos); err != nil {
		return nil, err
	}
	keys := make([]keyIndex, len(flat))

	totalWritten := offset
	for i, node := range flat {
		writtenForNode := 0
		if err := binary.Write(f, binary.LittleEndian, node.tombstone); err != nil {
			return nil, errors.Wrapf(err, "write tombstone for node %d", i)
		}
		writtenForNode += 1

		n, err := f.Write(node.value)
		if err != nil {
			return nil, errors.Wrapf(err, "write node %d", i)
		}
		writtenForNode += n

		keys[i] = keyIndex{
			valueStart: totalWritten,
			valueEnd:   totalWritten + writtenForNode,
			key:        node.key,
		}

		totalWritten += writtenForNode
	}

	return keys, nil
}

func (l *Memtable) flushDataCollection(f *os.File) ([]keyIndex, error) {
	flat := l.keyMulti.flattenInOrder()

	totalDataLength := totalValueSizeCollection(flat)
	offset := 12 // 2 bytes for level, 2 bytes for strategy, 8 bytes for this indicator itself
	indexPos := uint64(totalDataLength + offset)
	level := uint16(0) // always level zero on a new one

	if err := binary.Write(f, binary.LittleEndian, &level); err != nil {
		return nil, err
	}

	segStrat := SegmentStrategyFromString(l.strategy)
	if err := binary.Write(f, binary.LittleEndian, &segStrat); err != nil {
		return nil, err
	}
	if err := binary.Write(f, binary.LittleEndian, &indexPos); err != nil {
		return nil, err
	}
	keys := make([]keyIndex, len(flat))

	totalWritten := offset
	for i, node := range flat {
		writtenForNode := 0

		valueLen := uint64(len(node.values))
		if err := binary.Write(f, binary.LittleEndian, &valueLen); err != nil {
			return nil, errors.Wrapf(err, "write values len for node %d", i)
		}
		writtenForNode += 8

		for _, value := range node.values {
			if err := binary.Write(f, binary.LittleEndian, value.tombstone); err != nil {
				return nil, errors.Wrapf(err, "write tombstone for value on node %d", i)
			}
			writtenForNode += 1

			valueLen := uint64(len(value.value))
			if err := binary.Write(f, binary.LittleEndian, valueLen); err != nil {
				return nil, errors.Wrapf(err, "write len of value on node %d", i)
			}
			writtenForNode += 8

			n, err := f.Write(value.value)
			if err != nil {
				return nil, errors.Wrapf(err, "write value on node %d", i)
			}
			writtenForNode += n
		}

		keys[i] = keyIndex{
			valueStart: totalWritten,
			valueEnd:   totalWritten + writtenForNode,
			key:        node.key,
		}

		totalWritten += writtenForNode
	}

	return keys, nil
}

func totalValueSize(in []*binarySearchNode) int {
	var sum int
	for _, n := range in {
		sum += len(n.value)
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
	}

	return sum
}
