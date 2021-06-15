package lsmkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type compactorReplace struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1 *segmentCursorReplace
	c2 *segmentCursorReplace

	// the level matching those of the cursors
	currentLevel uint16

	secondaryIndexCount uint16

	w    io.WriteSeeker
	bufw *bufio.Writer
}

func newCompactorReplace(w io.WriteSeeker,
	c1, c2 *segmentCursorReplace, level, secondaryIndexCount uint16) *compactorReplace {
	return &compactorReplace{
		c1:                  c1,
		c2:                  c2,
		w:                   w,
		bufw:                bufio.NewWriterSize(w, 1e6),
		currentLevel:        level,
		secondaryIndexCount: secondaryIndexCount,
	}
}

func (c *compactorReplace) do() error {
	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	kis, err := c.writeKeys()
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	if err := c.writeIndex(kis); err != nil {
		return errors.Wrap(err, "write index")
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return errors.Wrap(err, "flush buffered")
	}

	dataEnd := uint64(kis[len(kis)-1].valueEnd)

	if err := c.writeHeader(c.currentLevel+1, 0, c.secondaryIndexCount, dataEnd); err != nil {
		return errors.Wrap(err, "write header")
	}

	return nil
}

func (c *compactorReplace) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, SegmentHeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

func (c *compactorReplace) writeKeys() ([]keyIndex, error) {
	key1, value1, err1 := c.c1.first()
	key2, value2, err2 := c.c2.first()

	// the (dummy) header was already written, this is our initial offset
	offset := SegmentHeaderSize

	var kis []keyIndex

	for {
		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {
			ki, err := c.writeIndividualNode(offset, key2, value2, err2 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (equal keys)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)

			// advance both!
			key1, value1, err1 = c.c1.next()
			key2, value2, err2 = c.c2.next()
			continue
		}

		if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			ki, err := c.writeIndividualNode(offset, key1, value1, err1 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (key1 smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)
			key1, value1, err1 = c.c1.next()
		} else {
			// key 2 is smaller
			ki, err := c.writeIndividualNode(offset, key2, value2, err2 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (key2 smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)

			key2, value2, err2 = c.c2.next()
		}
	}

	return kis, nil
}

func (c *compactorReplace) writeIndividualNode(offset int, key, value []byte,
	tombstone bool) (keyIndex, error) {
	out := keyIndex{}

	writtenForNode := 0
	if err := binary.Write(c.bufw, binary.LittleEndian, tombstone); err != nil {
		return out, errors.Wrap(err, "write tombstone for node")
	}
	writtenForNode += 1

	valueLength := uint64(len(value))
	if err := binary.Write(c.bufw, binary.LittleEndian, &valueLength); err != nil {
		return out, errors.Wrap(err, "write value length encoding for node")
	}
	writtenForNode += 8

	n, err := c.bufw.Write(value)
	if err != nil {
		return out, errors.Wrap(err, "write node")
	}
	writtenForNode += n

	keyLength := uint32(len(key))
	if err := binary.Write(c.bufw, binary.LittleEndian, &keyLength); err != nil {
		return out, errors.Wrapf(err, "write key length encoding for node")
	}
	writtenForNode += 4

	n, err = c.bufw.Write(key)
	if err != nil {
		return out, errors.Wrapf(err, "write node")
	}
	writtenForNode += n

	out = keyIndex{
		valueStart: offset,
		valueEnd:   offset + writtenForNode,
		key:        key,
	}

	return out, nil
}

func (c *compactorReplace) writeIndex(keys []keyIndex) error {
	keyNodes := make([]segmentindex.Node, len(keys))
	for i, key := range keys {
		keyNodes[i] = segmentindex.Node{
			Key:   key.key,
			Start: uint64(key.valueStart),
			End:   uint64(key.valueEnd),
		}
	}
	index := segmentindex.NewBalanced(keyNodes)

	indexBytes, err := index.MarshalBinary()
	if err != nil {
		return err
	}

	if _, err := c.bufw.Write(indexBytes); err != nil {
		return err
	}

	return nil
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *compactorReplace) writeHeader(level, version, secIndexCount uint16,
	startOfIndex uint64) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	if err := binary.Write(c.w, binary.LittleEndian, &level); err != nil {
		return err
	}
	if err := binary.Write(c.w, binary.LittleEndian, &version); err != nil {
		return err
	}
	if err := binary.Write(c.w, binary.LittleEndian, &secIndexCount); err != nil {
		return err
	}
	if err := binary.Write(c.w, binary.LittleEndian, SegmentStrategyReplace); err != nil {
		return err
	}
	if err := binary.Write(c.w, binary.LittleEndian, &startOfIndex); err != nil {
		return err
	}

	return nil
}
