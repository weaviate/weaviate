//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/weaviate/weaviate/usecases/memwatch"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/compactor"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/gobenc"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
	"github.com/weaviate/weaviate/entities/diskio"
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
	bufw compactor.Writer
	mw   *compactor.MemoryWriter

	offset int

	tombstonesToWrite *sroar.Bitmap
	tombstonesToClean *sroar.Bitmap

	// merged property lengths of c1+c2 as docID-sorted parallel arrays, read by
	// block re-encoding (via a cursor) and serialized directly — never a map.
	propLengthIds  []uint64
	propLengthLens []uint32

	invertedHeader *segmentindex.HeaderInverted

	docIdEncoder varenc.VarEncEncoder[uint64]
	tfEncoder    varenc.VarEncEncoder[uint64]

	allocChecker memwatch.AllocChecker

	k1, b, avgPropLen float64

	enableChecksumValidation bool

	segmentFile    *segmentindex.SegmentFile
	maxNewFileSize int64

	// reusable buffers so per-key/per-block compaction encoding allocates nothing
	writeBuf   [8]byte
	arena      keyArena
	encodeBufs compactorInvertedBuffers
}

func newCompactorInverted(w io.WriteSeeker,
	c1, c2 *segmentCursorInvertedReusable, level, secondaryIndexCount uint16,
	cleanupTombstones bool,
	k1, b, avgPropLen float64, maxNewFileSize int64,
	allocChecker memwatch.AllocChecker, enableChecksumValidation bool,
) *compactorInverted {
	observeWrite := monitoring.GetMetrics().FileIOWrites.With(prometheus.Labels{
		"operation": "compaction",
		"strategy":  StrategyInverted,
	})
	writeCB := func(written int64) {
		observeWrite.Observe(float64(written))
	}
	meteredW := diskio.NewMeteredWriter(w, writeCB)
	writer, mw := compactor.NewWriter(meteredW, maxNewFileSize)

	return &compactorInverted{
		c1:                       c1,
		c2:                       c2,
		w:                        meteredW,
		bufw:                     writer,
		mw:                       mw,
		currentLevel:             level,
		cleanupTombstones:        cleanupTombstones,
		secondaryIndexCount:      secondaryIndexCount,
		offset:                   0,
		k1:                       k1,
		b:                        b,
		avgPropLen:               avgPropLen,
		enableChecksumValidation: enableChecksumValidation,
		maxNewFileSize:           maxNewFileSize,
		allocChecker:             allocChecker,
		encodeBufs:               newCompactorInvertedBuffers(),
	}
}

func (c *compactorInverted) do(ctx context.Context) error {
	var err error

	if err := c.init(); err != nil {
		return errors.Wrap(err, "init")
	}

	c.segmentFile = segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)

	c.tombstonesToWrite, err = c.c1.segment.ReadOnlyTombstones()
	if err != nil {
		return errors.Wrap(err, "get tombstones")
	}

	c.tombstonesToClean, err = c.c2.segment.ReadOnlyTombstones()
	if err != nil {
		return errors.Wrap(err, "get tombstones")
	}

	ids1, lens1, err := c.c1.segment.getPropertyLengthsPairs()
	if err != nil {
		return errors.Wrap(err, "get property lengths")
	}

	ids2, lens2, err := c.c2.segment.getPropertyLengthsPairs()
	if err != nil {
		return errors.Wrap(err, "get property lengths")
	}

	// drop the property lengths of docs cleanupValues removes from the older segment
	c.propLengthIds, c.propLengthLens = mergePropLenPairs(ids1, lens1, ids2, lens2, c.tombstonesToClean)

	tombstones := c.computeTombstones()

	keysOffset := segmentindex.HeaderSize + segmentindex.SegmentInvertedDefaultHeaderSize + len(c.c1.segment.invertedHeader.DataFields)

	kis, err := c.writeKeys(ctx)
	if err != nil {
		return errors.Wrap(err, "write keys")
	}

	tombstoneOffset := c.offset
	_, err = c.writeTombstones(tombstones)
	if err != nil {
		return errors.Wrap(err, "write tombstones")
	}

	propertyLengthsOffset := c.offset
	_, err = c.writePropertyLengths()
	if err != nil {
		return errors.Wrap(err, "write property lengths")
	}
	treeOffset := uint64(c.offset)
	if err := c.writeIndices(kis, uint64(keysOffset)); err != nil {
		return errors.Wrap(err, "write index")
	}

	// flush buffered, so we can safely seek on underlying writer
	if c.mw == nil {
		if err := c.bufw.Flush(); err != nil {
			return fmt.Errorf("flush buffered: %w", err)
		}
	}

	c.invertedHeader.KeysOffset = uint64(keysOffset)
	c.invertedHeader.TombstoneOffset = uint64(tombstoneOffset)
	c.invertedHeader.PropertyLengthsOffset = uint64(propertyLengthsOffset)

	version := segmentindex.ChooseHeaderVersion(c.enableChecksumValidation)
	if err := compactor.WriteHeaders(c.mw, c.w, c.bufw, c.segmentFile, c.currentLevel, version,
		c.secondaryIndexCount, treeOffset, segmentindex.StrategyInverted, c.invertedHeader); err != nil {
		return errors.Wrap(err, "write header")
	}

	if _, err := c.segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write compactorMap segment checksum: %w", err)
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
	if _, err := c.segmentFile.BodyWriter().Write(buf); err != nil {
		return 0, err
	}

	if _, err := c.segmentFile.BodyWriter().Write(tombstonesBuffer); err != nil {
		return 0, err
	}
	c.offset += len(tombstonesBuffer) + 8
	return len(tombstonesBuffer) + 8, nil
}

// mergedPropertyLengthStats returns the merged segment's (count, average)
// property length from the live set in c.propLengthLens, which mergePropLenPairs
// has already stripped of the older segment's tombstoned docs and cross-segment
// duplicates. Deriving it from the array keeps the stored avgdl in step with
// what the segment physically holds.
func (c *compactorInverted) mergedPropertyLengthStats() (uint64, float64) {
	count := uint64(len(c.propLengthLens))
	if count == 0 {
		return 0, 0
	}
	var sum uint64
	for _, l := range c.propLengthLens {
		sum += uint64(l)
	}
	return count, float64(sum) / float64(count)
}

func (c *compactorInverted) writePropertyLengths() (int, error) {
	encoded, err := gobenc.EncodePairs(c.propLengthIds, c.propLengthLens)
	if err != nil {
		return 0, err
	}

	count, average := c.mergedPropertyLengthStats()

	buf := make([]byte, 8)

	binary.LittleEndian.PutUint64(buf, math.Float64bits(average))
	if _, err := c.segmentFile.BodyWriter().Write(buf); err != nil {
		return 0, err
	}

	binary.LittleEndian.PutUint64(buf, count)
	if _, err := c.segmentFile.BodyWriter().Write(buf); err != nil {
		return 0, err
	}

	binary.LittleEndian.PutUint64(buf, uint64(len(encoded)))
	if _, err := c.segmentFile.BodyWriter().Write(buf); err != nil {
		return 0, err
	}

	if _, err := c.segmentFile.BodyWriter().Write(encoded); err != nil {
		return 0, err
	}
	c.offset += len(encoded) + 8 + 8 + 8
	return len(encoded) + 8 + 8 + 8, nil
}

func (c *compactorInverted) writeKeys(ctx context.Context) ([]segmentindex.KeyRedux, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset

	kis := make([]segmentindex.KeyRedux, 0, c.c1.segment.index.KeyCount()+c.c2.segment.index.KeyCount())
	sim := newSortedMapMerger()

	for i := 0; ; i++ {
		if i%compactor.AbortCheckEveryN == 0 {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("merge keys: %w", err)
			}
		}

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

			ki, err := c.writeIndividualNode(c.offset, key2, mergedPairs)
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
				ki, err := c.writeIndividualNode(c.offset, key1, values)
				if err != nil {
					return nil, errors.Wrap(err, "write individual node (key1 smaller)")
				}

				c.offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			key1, value1, _ = c.c1.next()
		} else {
			// key 2 is smaller
			ki, err := c.writeIndividualNode(c.offset, key2, value2)
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
	values []MapPair,
) (segmentindex.KeyRedux, error) {
	// Copy the key: the cursor reuses its key buffer across iterations, so
	// keeping the slice would let a later next() overwrite it
	// (https://github.com/weaviate/weaviate/issues/3517).
	keyCopy := c.arena.CopyKey(key)

	// fresh view per term: its docIDs ascend, so lookups stay amortized O(1)
	view := propLengthsView{ids: c.propLengthIds, lens: c.propLengthLens}

	return segmentInvertedNode{
		values:      values,
		primaryKey:  keyCopy,
		offset:      offset,
		propLengths: &view,
	}.KeyIndexAndWriteToCompaction(c.segmentFile.BodyWriter(), c.writeBuf[:], &c.encodeBufs,
		c.docIdEncoder, c.tfEncoder, c.k1, c.b, c.avgPropLen)
}

func (c *compactorInverted) writeIndices(keys []segmentindex.KeyRedux, dataStartOffset uint64) error {
	// KeyIndexAndWriteToCompaction emits no secondary keys and MarshalSortedKeys
	// serializes none, so this path is only valid without secondary indexes —
	// which inverted (BM25 postings) never has.
	if c.secondaryIndexCount > 0 {
		return fmt.Errorf("inverted compaction does not support secondary indexes")
	}
	_, err := segmentindex.MarshalSortedKeys(c.segmentFile.BodyWriter(), keys, dataStartOffset)
	return err
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

func (c *compactorInverted) computeTombstones() *sroar.Bitmap {
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
