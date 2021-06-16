package lsmkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
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

	if err := c.writeIndices(kis); err != nil {
		return errors.Wrap(err, "write indices")
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
	res1, err1 := c.c1.firstWithAllKeys()
	res2, err2 := c.c2.firstWithAllKeys()

	// the (dummy) header was already written, this is our initial offset
	offset := SegmentHeaderSize

	var kis []keyIndex

	for {
		if res1.key == nil && res2.key == nil {
			break
		}
		if bytes.Equal(res1.key, res2.key) {
			ki, err := c.writeIndividualNode(offset, res2.key, res2.value,
				res2.secondaryKeys, err2 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (equal keys)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)

			// advance both!
			res1, err1 = c.c1.nextWithAllKeys()
			res2, err2 = c.c2.nextWithAllKeys()
			continue
		}

		if (res1.key != nil && bytes.Compare(res1.key, res2.key) == -1) || res2.key == nil {
			// key 1 is smaller
			ki, err := c.writeIndividualNode(offset, res1.key, res1.value,
				res1.secondaryKeys, err1 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (res1.key smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)
			res1, err1 = c.c1.nextWithAllKeys()
		} else {
			// key 2 is smaller
			ki, err := c.writeIndividualNode(offset, res2.key, res2.value,
				res2.secondaryKeys, err2 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (res2.key smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)

			res2, err2 = c.c2.nextWithAllKeys()
		}
	}

	return kis, nil
}

func (c *compactorReplace) writeIndividualNode(offset int, key, value []byte,
	secondaryKeys [][]byte, tombstone bool) (keyIndex, error) {
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

	for j := 0; j < int(c.secondaryIndexCount); j++ {
		var secondaryKeyLength uint32
		if j < len(secondaryKeys) {
			secondaryKeyLength = uint32(len(secondaryKeys[j]))
		}

		// write the key length in any case
		if err := binary.Write(c.bufw, binary.LittleEndian, &secondaryKeyLength); err != nil {
			return out, errors.Wrapf(err, "write secondary key length encoding for node")
		}
		writtenForNode += 4

		if secondaryKeyLength == 0 {
			// we're done here
			continue
		}

		// only write the key if it exists
		n, err = c.bufw.Write(secondaryKeys[j])
		if err != nil {
			return out, errors.Wrapf(err, "write secondary key %d for node", j)
		}
		writtenForNode += n
	}

	out = keyIndex{
		valueStart:    offset,
		valueEnd:      offset + writtenForNode,
		key:           key,
		secondaryKeys: secondaryKeys,
	}

	return out, nil
}

func (c *compactorReplace) writeIndices(keys []keyIndex) error {
	indices := &segmentIndices{
		keys:                keys,
		secondaryIndexCount: c.secondaryIndexCount,
	}

	_, err := indices.WriteTo(c.bufw)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *compactorReplace) writeHeader(level, version, secondaryIndices uint16,
	startOfIndex uint64) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentHeader{
		level:            level,
		version:          version,
		secondaryIndices: secondaryIndices,
		strategy:         SegmentStrategyReplace,
		indexStart:       startOfIndex,
	}

	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}
