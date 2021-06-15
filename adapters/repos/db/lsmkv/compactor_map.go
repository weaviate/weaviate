package lsmkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type compactorMap struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1 *segmentCursorCollection
	c2 *segmentCursorCollection

	// the level matching those of the cursors
	currentLevel        uint16
	secondaryIndexCount uint16

	w    io.WriteSeeker
	bufw *bufio.Writer
}

func newCompactorMapCollection(w io.WriteSeeker,
	c1, c2 *segmentCursorCollection, level, secondaryIndexCount uint16) *compactorMap {
	return &compactorMap{
		c1:                  c1,
		c2:                  c2,
		w:                   w,
		bufw:                bufio.NewWriterSize(w, 1e6),
		currentLevel:        level,
		secondaryIndexCount: secondaryIndexCount,
	}
}

func (c *compactorMap) do() error {
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

	if err := c.writeHeader(c.currentLevel+1, 0, c.secondaryIndexCount,
		dataEnd); err != nil {
		return errors.Wrap(err, "write header")
	}

	return nil
}

func (c *compactorMap) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, SegmentHeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

func (c *compactorMap) writeKeys() ([]keyIndex, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset
	offset := SegmentHeaderSize

	var kis []keyIndex

	for {
		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {
			values := append(value1, value2...)
			valuesMerged, err := newMapDecoder().DoPartial(values)
			if err != nil {
				return nil, err
			}

			var mergedEncoded []value
			for _, merged := range valuesMerged {
				encoded, err := newMapEncoder().Do(merged)
				if err != nil {
					return nil, err
				}

				mergedEncoded = append(mergedEncoded, encoded...)
			}

			ki, err := c.writeIndividualNode(offset, key2, mergedEncoded)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (equal keys)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)

			// advance both!
			key1, value1, _ = c.c1.next()
			key2, value2, _ = c.c2.next()
			continue
		}

		if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			ki, err := c.writeIndividualNode(offset, key1, value1)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (key1 smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)
			key1, value1, _ = c.c1.next()
		} else {
			// key 2 is smaller
			ki, err := c.writeIndividualNode(offset, key2, value2)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (key2 smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)

			key2, value2, _ = c.c2.next()
		}
	}

	return kis, nil
}

func (c *compactorMap) writeIndividualNode(offset int, key []byte,
	values []value) (keyIndex, error) {
	out := keyIndex{}

	writtenForNode := 0
	valueLen := uint64(len(values))
	if err := binary.Write(c.bufw, binary.LittleEndian, &valueLen); err != nil {
		return out, errors.Wrapf(err, "write values len for node")
	}
	writtenForNode += 8

	for i, value := range values {
		if err := binary.Write(c.bufw, binary.LittleEndian, value.tombstone); err != nil {
			return out, errors.Wrapf(err, "write tombstone for value %d", i)
		}
		writtenForNode += 1

		valueLen := uint64(len(value.value))
		if err := binary.Write(c.bufw, binary.LittleEndian, valueLen); err != nil {
			return out, errors.Wrapf(err, "write len of value %d", i)
		}
		writtenForNode += 8

		n, err := c.bufw.Write(value.value)
		if err != nil {
			return out, errors.Wrapf(err, "write value %d", i)
		}
		writtenForNode += n
	}

	keyLength := uint32(len(key))
	if err := binary.Write(c.bufw, binary.LittleEndian, &keyLength); err != nil {
		return out, errors.Wrapf(err, "write key length encoding for node")
	}
	writtenForNode += 4

	n, err := c.bufw.Write(key)
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

func (c *compactorMap) writeIndex(keys []keyIndex) error {
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
func (c *compactorMap) writeHeader(level, version, secondaryIndices uint16,
	startOfIndex uint64) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentHeader{
		level:            level,
		version:          version,
		secondaryIndices: secondaryIndices,
		strategy:         SegmentStrategyMapCollection,
		indexStart:       startOfIndex,
	}

	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}
