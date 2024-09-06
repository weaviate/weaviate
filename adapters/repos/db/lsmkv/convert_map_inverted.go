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

package lsmkv

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type converterMapInverted struct {
	c1 *segmentCursorCollectionReusable

	// Tells if tombstones or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupTombstones bool

	w    io.WriteSeeker
	bufw *bufio.Writer

	scratchSpacePath string

	offset int

	tombstonesToWrite *sroar.Bitmap

	keysLen uint64
}

func newConverterMapInvertedCollection(w io.WriteSeeker,
	c1 *segmentCursorCollectionReusable,
	scratchSpacePath string, cleanupTombstones bool,
) *converterMapInverted {
	tombstonesToWrite := sroar.NewBitmap()
	return &converterMapInverted{
		c1:                c1,
		w:                 w,
		bufw:              bufio.NewWriterSize(w, 256*1024),
		cleanupTombstones: cleanupTombstones,
		scratchSpacePath:  scratchSpacePath,
		tombstonesToWrite: tombstonesToWrite,
	}
}

func (c *converterMapInverted) do() error {
	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	c.offset = segmentindex.HeaderSize

	err := c.writeKeyValueLen()
	if err != nil {
		return errors.Wrap(err, "write key and value length")
	}
	c.offset += 2 + 2 // 2 bytes for key length, 2 bytes for value length

	kis, tombstones, err := c.writeKeys()
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	tombstoneSize, err := c.writeTombstones(tombstones)
	if err != nil {
		return errors.Wrap(err, "write tombstones")
	}

	if err := c.writeIndices(kis); err != nil {
		return errors.Wrap(err, "write index")
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return errors.Wrap(err, "flush buffered")
	}

	var dataEnd uint64 = segmentindex.HeaderSize + 2 + 2 + 8 + 8 + uint64(tombstoneSize)
	if len(kis) > 0 {
		dataEnd = uint64(kis[len(kis)-1].ValueEnd) + 8 + uint64(tombstoneSize)
	}
	if err := c.writeHeader(c.c1.segment.level, 0, c.c1.segment.secondaryIndexCount,
		dataEnd); err != nil {
		return errors.Wrap(err, "write header")
	}

	if err := c.writeKeysLength(); err != nil {
		return errors.Wrap(err, "write keys length")
	}

	return nil
}

func (c *converterMapInverted) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}

	return nil
}

// writeKeysLength assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *converterMapInverted) writeKeysLength() error {
	if _, err := c.w.Seek(16+2+2, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, c.keysLen)

	_, err := c.w.Write(buf)

	return err
}

func (c *converterMapInverted) writeKeyValueLen() error {
	// write default key and value length
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, defaultInvertedKeyLength)
	if _, err := c.bufw.Write(buf[:2]); err != nil {
		return err
	}

	binary.LittleEndian.PutUint16(buf, defaultInvertedValueLength)
	if _, err := c.bufw.Write(buf[:2]); err != nil {
		return err
	}

	return nil
}

func (c *converterMapInverted) writeTombstones(tombstones *sroar.Bitmap) (int, error) {
	tombstonesBuffer := make([]byte, 0)

	if tombstones != nil && tombstones.GetCardinality() > 0 {
		tombstonesBuffer = tombstones.ToBuffer()
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(tombstonesBuffer)))
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	if _, err := c.bufw.Write(tombstonesBuffer); err != nil {
		return 0, err
	}

	return len(tombstonesBuffer), nil
}

func (c *converterMapInverted) writeKeys() ([]segmentindex.Key, *sroar.Bitmap, error) {
	// placeholder for the keys length
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 0)
	if _, err := c.bufw.Write(buf); err != nil {
		return nil, nil, err
	}

	c.offset += 8

	key1, value1, _ := c.c1.first()

	// the (dummy) header was already written, this is our initial offset

	var kis []segmentindex.Key

	for key1 != nil {
		if values, skip := c.cleanupValues(value1); !skip {
			ki, err := c.writeIndividualNodeLegacy(c.offset, key1, values)
			if err != nil {
				return nil, nil, errors.Wrap(err, "write individual node (key1 smaller)")
			}
			c.keysLen += uint64(ki.ValueEnd - ki.ValueStart)
			c.offset = ki.ValueEnd
			kis = append(kis, ki)
		}
		key1, value1, _ = c.c1.next()
	}

	tombstones := c.computeTombstones()
	return kis, tombstones, nil
}

func (c *converterMapInverted) writeIndividualNodeLegacy(offset int, key []byte,
	values []value,
) (segmentindex.Key, error) {
	// NOTE: There are no guarantees in the cursor logic that any memory is valid
	// for more than a single iteration. Every time you call next() to advance
	// the cursor, any memory might be reused.
	//
	// This includes the key buffer which was the cause of
	// https://github.com/weaviate/weaviate/issues/3517
	//
	// A previous logic created a new assignment in each iteration, but thatwas
	// not an explicit guarantee. A change in v1.21 (for pread/mmap) added a
	// reusable buffer for the key which surfaced this bug.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	return segmentInvertedNode{
		values:     values,
		primaryKey: keyCopy,
		offset:     offset,
	}.KeyIndexAndWriteToLegacy(c.bufw)
}

func (c *converterMapInverted) writeIndices(keys []segmentindex.Key) error {
	indices := segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.c1.segment.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
	}

	_, err := indices.WriteTo(c.bufw)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *converterMapInverted) writeHeader(level, version, secondaryIndices uint16,
	startOfIndex uint64,
) error {
	if _, err := c.w.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	h := &segmentindex.Header{
		Level:            level,
		Version:          version,
		SecondaryIndices: secondaryIndices,
		Strategy:         segmentindex.StrategyInverted,
		IndexStart:       startOfIndex,
	}

	if _, err := h.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}

// Removes values with tombstone set from input slice. Output slice may be smaller than input one.
// Returned skip of true means there are no values left (key can be omitted in segment)
// WARN: method can alter input slice by swapping its elements and reducing length (not capacity)
func (c *converterMapInverted) cleanupValues(values []value) (vals []value, skip bool) {
	// Reuse input slice not to allocate new memory
	// Rearrange slice in a way that tombstoned values are moved to the end
	// and reduce slice's length.
	last := 0
	for i := 0; i < len(values); i++ {
		if values[i].tombstone {
			docId := binary.BigEndian.Uint64(values[i].value[0:8])
			c.tombstonesToWrite.Set(docId)
		} else if !values[i].tombstone {
			// Swap both elements instead overwritting `last` by `i`.
			// Overwrite would result in `values[last].value` pointing to the same slice
			// as `values[i].value`.
			// If `values` slice is reused by multiple nodes (as it happens for map cursors
			// `segmentInvertedReusable` using `segmentNode` as buffer)
			// populating slice `values[i].value` would overwrite slice `values[last].value`.
			// Swaps makes sure `values[i].value` and `values[last].value` point to different slices.
			values[last], values[i] = values[i], values[last]
			last++
		}
	}

	if last == 0 {
		return nil, true
	}
	return values[:last], false
}

func (c *converterMapInverted) computeTombstones() *sroar.Bitmap {
	if c.cleanupTombstones { // no tombstones to write
		return sroar.NewBitmap()
	}

	return c.tombstonesToWrite
}
