package lsmkv

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
)

type Memtable struct {
	sync.RWMutex
	key          *binarySearchTree
	primaryIndex *binarySearchTree
	commitlog    *commitLogger
	size         uint64
	path         string
}

func newMemtable(path string) (*Memtable, error) {
	cl, err := newCommitLogger(path)
	if err != nil {
		return nil, errors.Wrap(err, "init commit logger")
	}

	return &Memtable{
		key:          &binarySearchTree{},
		primaryIndex: &binarySearchTree{}, // todo, sort upfront
		commitlog:    cl,
		path:         path,
	}, nil
}

type keyIndex struct {
	hash       []byte
	valueStart int
	valueEnd   int
}

func (l *Memtable) get(key []byte) ([]byte, error) {
	l.RLock()
	defer l.RUnlock()

	v, err := l.key.get(key)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (l *Memtable) put(key, value []byte) error {
	l.Lock()
	defer l.Unlock()
	if err := l.commitlog.put(key, value); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.key.insert(key, value)
	l.size += uint64(len(key))
	l.size += uint64(len(value))

	return nil
}

func (l *Memtable) setTombstone(key []byte) error {
	l.Lock()
	defer l.Unlock()

	if err := l.commitlog.setTombstone(key); err != nil {
		return errors.Wrap(err, "write into commit log")
	}

	l.key.setTombstone(key)

	return nil
}

func (l *Memtable) Size() uint64 {
	l.RLock()
	defer l.RUnlock()

	return l.size
}

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

	flat := l.key.flattenInOrder()

	totalDataLength := totalValueSize(flat)
	perObjectAdditions := len(flat) * 1 // 1 byte for the tombstone
	offset := 10                        // 2 bytes for level, 8 bytes for this indicator itself
	indexPos := uint64(totalDataLength + perObjectAdditions + offset)
	level := uint16(0) // always level zero on a new one

	if err := binary.Write(f, binary.LittleEndian, &level); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, &indexPos); err != nil {
		return err
	}

	keys := make([]keyIndex, len(flat))

	totalWritten := 10 // offset level + indexPos unit64
	for i, node := range flat {
		writtenForNode := 0
		if err := binary.Write(f, binary.LittleEndian, node.tombstone); err != nil {
			return errors.Wrapf(err, "write tombstone for node %d", i)
		}
		writtenForNode += 1

		n, err := f.Write(node.value)
		if err != nil {
			return errors.Wrapf(err, "write node %d", i)
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

func totalValueSize(in []*binarySearchNode) int {
	var sum int
	for _, n := range in {
		sum += len(n.value)
	}

	return sum
}
