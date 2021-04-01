package lsmkv

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
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

	case StrategyCollection:
		if keys, err = l.flushDataCollection(f); err != nil {
			return err
		}

	}
	// now sort keys according to their hashes for an efficient binary search
	sort.Slice(keys, func(a, b int) bool {
		return bytes.Compare(keys[a].hash, keys[b].hash) < 0
	})

	// now write all the keys with "links" to the values
	// delimit a key with \xFF (obviously needs a better mechanism to protect against the data containing the delimter byte)
	for _, key := range keys {
		f.Write(key.hash)

		start := uint64(key.valueStart)
		end := uint64(key.valueEnd)
		if err := binary.Write(f, binary.LittleEndian, &start); err != nil {
			return err
		}
		if err := binary.Write(f, binary.LittleEndian, &end); err != nil {
			return err
		}
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

		hasher := murmur3.New128()
		hasher.Write(node.key)
		hash := hasher.Sum(nil)
		keys[i] = keyIndex{
			valueStart: totalWritten,
			valueEnd:   totalWritten + writtenForNode,
			hash:       hash,
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
	if err := binary.Write(f, binary.LittleEndian, SegmentStrategyCollection); err != nil {
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

			if !value.tombstone {
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
		}

		hasher := murmur3.New128()
		hasher.Write(node.key)
		hash := hasher.Sum(nil)
		keys[i] = keyIndex{
			valueStart: totalWritten,
			valueEnd:   totalWritten + writtenForNode,
			hash:       hash,
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
			sum += 8 // uint64 to indicate value length
			sum += 1 // bool to indicate value tombstone
			sum += len(v.value)
		}
	}

	return sum
}
