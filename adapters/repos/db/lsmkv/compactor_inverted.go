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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"maps"
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type compactorInverted struct {
	// c1 is always the older segment, so when there is a conflict c2 wins
	// (because of the replace strategy)
	c1 *segmentCursorInvertedReusable
	c2 *segmentCursorInvertedReusable

	// the level matching those of the cursors
	currentLevel        uint16
	secondaryIndexCount uint16
	// Tells if tombstones or keys without corresponding values
	// can be removed from merged segment.
	// (left segment is root (1st) one, keepTombstones is off for bucket)
	cleanupTombstones bool

	w    io.WriteSeeker
	bufw *bufio.Writer

	scratchSpacePath string

	offset int

	tombstonesToWrite *sroar.Bitmap
	tombstonesToClean *sroar.Bitmap

	propertyLengthsToWrite map[uint64]uint32
	propertyLengthsToClean map[uint64]uint32

	invertedHeader *segmentindex.HeaderInverted

	docIdEncoder varenc.VarEncEncoder[uint64]
	tfEncoder    varenc.VarEncEncoder[uint64]

	k1, b, avgPropLen float64
}

func newCompactorInverted(w io.WriteSeeker,
	c1, c2 *segmentCursorInvertedReusable, level, secondaryIndexCount uint16,
	scratchSpacePath string, cleanupTombstones bool,
	k1, b, avgPropLen float64,
) *compactorInverted {
	return &compactorInverted{
		c1:                  c1,
		c2:                  c2,
		w:                   w,
		bufw:                bufio.NewWriterSize(w, 256*1024),
		currentLevel:        level,
		cleanupTombstones:   cleanupTombstones,
		secondaryIndexCount: secondaryIndexCount,
		scratchSpacePath:    scratchSpacePath,
		offset:              0,
		k1:                  k1,
		b:                   b,
		avgPropLen:          avgPropLen,
	}
}

func (c *compactorInverted) do() error {
	var err error

	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	c.tombstonesToWrite, err = c.c1.segment.ReadOnlyTombstones()
	if err != nil {
		return errors.Wrap(err, "get tombstones")
	}

	c.tombstonesToClean, err = c.c2.segment.ReadOnlyTombstones()
	if err != nil {
		return errors.Wrap(err, "get tombstones")
	}

	propertyLengthsToWrite, err := c.c1.segment.GetPropertyLengths()
	if err != nil {
		return errors.Wrap(err, "get property lengths")
	}

	propertyLengthsToClean, err := c.c2.segment.GetPropertyLengths()
	if err != nil {
		return errors.Wrap(err, "get property lengths")
	}

	c.propertyLengthsToWrite = make(map[uint64]uint32, len(propertyLengthsToWrite))
	c.propertyLengthsToClean = make(map[uint64]uint32, len(propertyLengthsToClean))

	maps.Copy(c.propertyLengthsToWrite, propertyLengthsToWrite)
	maps.Copy(c.propertyLengthsToClean, propertyLengthsToClean)

	tombstones := c.computeTombstonesAndPropLengths()

	kis, err := c.writeKeys()
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	tombstoneOffset := c.offset
	_, err = c.writeTombstones(tombstones)
	if err != nil {
		return errors.Wrap(err, "write tombstones")
	}

	propertyLengthsOffset := c.offset
	_, err = c.writePropertyLengths(c.propertyLengthsToWrite)
	if err != nil {
		return errors.Wrap(err, "write property lengths")
	}
	treeOffset := uint64(c.offset)
	if err := c.writeIndices(kis); err != nil {
		return errors.Wrap(err, "write index")
	}

	// flush buffered, so we can safely seek on underlying writer
	if err := c.bufw.Flush(); err != nil {
		return errors.Wrap(err, "flush buffered")
	}

	// TODO: checksums currently not supported for StrategyInverted,
	//       which was introduced with segmentindex.SegmentV1. When
	//       support is added, we can bump this header version to 1.
	version := uint16(0)
	if err := c.writeHeader(c.currentLevel, version, c.secondaryIndexCount,
		treeOffset); err != nil {
		return errors.Wrap(err, "write header")
	}

	if err := c.writeInvertedHeader(tombstoneOffset, propertyLengthsOffset); err != nil {
		return errors.Wrap(err, "write keys length")
	}

	return nil
}

func (c *compactorInverted) init() error {
	// write a dummy header, we don't know the contents of the actual header yet,
	// we will seek to the beginning and overwrite the actual header at the very
	// end

	if len(c.c1.segment.invertedHeader.DataFields) != len(c.c2.segment.invertedHeader.DataFields) {
		return errors.Errorf("inverted header data fields mismatch: %d != %d",
			len(c.c1.segment.invertedHeader.DataFields),
			len(c.c2.segment.invertedHeader.DataFields))
	}

	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return errors.Wrap(err, "write empty header")
	}
	if _, err := c.bufw.Write(make([]byte, segmentindex.SegmentInvertedDefaultHeaderSize+len(c.c1.segment.invertedHeader.DataFields))); err != nil {
		return errors.Wrap(err, "write empty inverted header")
	}
	c.offset = segmentindex.HeaderSize + segmentindex.SegmentInvertedDefaultHeaderSize + len(c.c1.segment.invertedHeader.DataFields)

	c.invertedHeader = &segmentindex.HeaderInverted{
		// TODO: checksums currently not supported for StrategyInverted,
		//       which was introduced with segmentindex.SegmentV1. When
		//       support is added, we can bump this header version to 1.
		Version:               0,
		KeysOffset:            uint64(c.offset),
		TombstoneOffset:       0,
		PropertyLengthsOffset: 0,
		BlockSize:             uint8(segmentindex.SegmentInvertedDefaultBlockSize),
		DataFieldCount:        uint8(len(c.c1.segment.invertedHeader.DataFields)),
		DataFields:            c.c1.segment.invertedHeader.DataFields,
	}

	c.docIdEncoder = varenc.GetVarEncEncoder64(c.invertedHeader.DataFields[0])
	c.docIdEncoder.Init(terms.BLOCK_SIZE)
	c.tfEncoder = varenc.GetVarEncEncoder64(c.invertedHeader.DataFields[1])
	c.tfEncoder.Init(terms.BLOCK_SIZE)

	return nil
}

func (c *compactorInverted) writeTombstones(tombstones *sroar.Bitmap) (int, error) {
	tombstonesBuffer := make([]byte, 0)

	if tombstones != nil && !tombstones.IsEmpty() {
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
	c.offset += len(tombstonesBuffer) + 8
	return len(tombstonesBuffer) + 8, nil
}

func (c *compactorInverted) combinePropertyLengths() (uint64, float64) {
	count := c.c1.segment.invertedData.avgPropertyLengthsCount + c.c2.segment.invertedData.avgPropertyLengthsCount
	average := c.c1.segment.invertedData.avgPropertyLengthsAvg * (float64(c.c1.segment.invertedData.avgPropertyLengthsCount) / float64(count))
	average += c.c2.segment.invertedData.avgPropertyLengthsAvg * (float64(c.c2.segment.invertedData.avgPropertyLengthsCount) / float64(count))

	return count, average
}

func (c *compactorInverted) writePropertyLengths(propLengths map[uint64]uint32) (int, error) {
	b := new(bytes.Buffer)

	e := gob.NewEncoder(b)

	// Encoding the map
	err := e.Encode(propLengths)
	if err != nil {
		return 0, err
	}

	count, average := c.combinePropertyLengths()

	buf := make([]byte, 8)

	binary.LittleEndian.PutUint64(buf, math.Float64bits(average))
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	binary.LittleEndian.PutUint64(buf, count)
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	binary.LittleEndian.PutUint64(buf, uint64(b.Len()))
	if _, err := c.bufw.Write(buf); err != nil {
		return 0, err
	}

	if _, err := c.bufw.Write(b.Bytes()); err != nil {
		return 0, err
	}
	c.offset += b.Len() + 8 + 8 + 8
	return b.Len() + 8 + 8 + 8, nil
}

func (c *compactorInverted) writeKeys() ([]segmentindex.Key, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset

	var kis []segmentindex.Key
	sim := newSortedMapMerger()

	for {
		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {

			value1Clean, _ := c.cleanupValues(value1)

			sim.reset([][]MapPair{value1Clean, value2})
			mergedPairs, err := sim.
				doKeepTombstonesReusable()
			if err != nil {
				return nil, err
			}

			if len(mergedPairs) == 0 {
				// skip key if no values left
				key1, value1, _ = c.c1.next()
				key2, value2, _ = c.c2.next()
				continue
			}

			ki, err := c.writeIndividualNode(c.offset, key2, mergedPairs, c.propertyLengthsToWrite)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (equal keys)")
			}

			c.offset = ki.ValueEnd
			kis = append(kis, ki)

			// advance both!
			key1, value1, _ = c.c1.next()
			key2, value2, _ = c.c2.next()
			continue
		}

		if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			if values, skip := c.cleanupValues(value1); !skip {
				ki, err := c.writeIndividualNode(c.offset, key1, values, c.propertyLengthsToWrite)
				if err != nil {
					return nil, errors.Wrap(err, "write individual node (key1 smaller)")
				}

				c.offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			key1, value1, _ = c.c1.next()
		} else {
			// key 2 is smaller
			ki, err := c.writeIndividualNode(c.offset, key2, value2, c.propertyLengthsToWrite)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (key2 smaller)")
			}

			c.offset = ki.ValueEnd
			kis = append(kis, ki)

			key2, value2, _ = c.c2.next()
		}
	}

	return kis, nil
}

func (c *compactorInverted) writeIndividualNode(offset int, key []byte,
	values []MapPair, propertyLengths map[uint64]uint32,
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
		values:      values,
		primaryKey:  keyCopy,
		offset:      offset,
		propLengths: propertyLengths,
	}.KeyIndexAndWriteTo(c.bufw, c.docIdEncoder, c.tfEncoder, c.k1, c.b, c.avgPropLen)
}

func (c *compactorInverted) writeIndices(keys []segmentindex.Key) error {
	indices := segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
		ObserveWrite: monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
			"strategy":  StrategyInverted,
			"operation": "writeIndices",
		}),
	}

	_, err := indices.WriteTo(c.bufw)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *compactorInverted) writeHeader(level, version, secondaryIndices uint16,
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

// writeInvertedHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *compactorInverted) writeInvertedHeader(tombstoneOffset, propertyLengthsOffset int) error {
	if _, err := c.w.Seek(16, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write inverted header")
	}

	c.invertedHeader.TombstoneOffset = uint64(tombstoneOffset)
	c.invertedHeader.PropertyLengthsOffset = uint64(propertyLengthsOffset)

	if _, err := c.invertedHeader.WriteTo(c.w); err != nil {
		return err
	}

	return nil
}

// Removes values with tombstone set from input slice. Output slice may be smaller than input one.
// Returned skip of true means there are no values left (key can be omitted in segment)
// WARN: method can alter input slice by swapping its elements and reducing length (not capacity)
func (c *compactorInverted) cleanupValues(values []MapPair) (vals []MapPair, skip bool) {
	// Reuse input slice not to allocate new memory
	// Rearrange slice in a way that tombstoned values are moved to the end
	// and reduce slice's length.
	last := 0
	for i := 0; i < len(values); i++ {
		docId := binary.BigEndian.Uint64(values[i].Key)
		if !(c.tombstonesToClean != nil && c.tombstonesToClean.Contains(docId)) {
			values[last], values[i] = values[i], values[last]
			last++
		}
	}

	if last == 0 {
		return nil, true
	}
	return values[:last], false
}

func (c *compactorInverted) computeTombstonesAndPropLengths() *sroar.Bitmap {
	maps.Copy(c.propertyLengthsToWrite, c.propertyLengthsToClean)

	if c.cleanupTombstones { // no tombstones to write
		return sroar.NewBitmap()
	}
	if c.tombstonesToWrite == nil {
		return c.tombstonesToClean
	}
	if c.tombstonesToClean == nil {
		return c.tombstonesToWrite
	}

	return sroar.Or(c.tombstonesToWrite, c.tombstonesToClean)
}
