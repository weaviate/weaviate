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

	propertyLengthsToWrite map[uint64]uint32

	invertedHeader *segmentindex.HeaderInverted

	docIdEncoder varenc.VarEncEncoder[uint64]
	tfEncoder    varenc.VarEncEncoder[uint64]

	allocChecker memwatch.AllocChecker

	k1, b, avgPropLen float64

	enableChecksumValidation bool

	// writeV2 gates the V2 flat-column property-length OUTPUT (default off,
	// reader-ahead-of-writer). When set, the compaction reads each input per its
	// OWN Version (V0 gob / V2 flat) and writes a V2 flat-column section. When
	// off, it writes the legacy V0 gob section. The per-input-Version READ runs
	// regardless of writeV2 so a V0+V2 mixed input is always merged correctly;
	// writeV2 only chooses the OUTPUT format.
	writeV2 bool

	// propLengthRunToWrite is the merged, sorted, LOSSLESS uint32 (docID,length)
	// run produced by the per-input-Version read in do(). It is the source of the
	// persisted exact per-doc length section on BOTH output paths -- the V2 flat
	// column AND the V0 gob -- so a >65535 doc round-trips verbatim through any
	// compaction (no uint16 clamp). The clamped propertyLengthsToWrite map feeds
	// ONLY the block-impact MaxImpactPropLength (WAND) path.
	propLengthRunToWrite propLengthRun

	segmentFile    *segmentindex.SegmentFile
	maxNewFileSize int64
}

func newCompactorInverted(w io.WriteSeeker,
	c1, c2 *segmentCursorInvertedReusable, level, secondaryIndexCount uint16,
	cleanupTombstones bool,
	k1, b, avgPropLen float64, maxNewFileSize int64,
	allocChecker memwatch.AllocChecker, enableChecksumValidation bool,
	writeV2 bool,
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
		writeV2:                  writeV2,
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

	// Two-pointer merge of the two segments' resident sorted (docID, length) runs,
	// replacing the former getPropertyLengths()+maps.Copy path. c1 is the left
	// (older) segment and c2 the right (newer) one; on an overlapping docID the
	// newer (c2) value wins, exactly as the previous maps.Copy(write <- clean)
	// did. The merged result is the map the rest of the compactor already consumes.
	olderDocIDs, olderValues, err := c.c1.segment.snapshotPropLengthSlices()
	if err != nil {
		return errors.Wrap(err, "get property lengths")
	}
	newerDocIDs, newerValues, err := c.c2.segment.snapshotPropLengthSlices()
	if err != nil {
		return errors.Wrap(err, "get property lengths")
	}

	c.propertyLengthsToWrite = mergePropertyLengthRuns(
		olderDocIDs, olderValues, newerDocIDs, newerValues,
	)

	// Per-input-Version LOSSLESS read for the persisted exact-length section (G6).
	// Each input's property-length section is read per that input's OWN
	// HeaderInverted.Version (V0 -> lossless gob uint32, V2 -> flat column), then
	// merged NEWER-wins into a single sorted uint32 run. The len(DataFields) guard
	// in init() does NOT catch a V0/V2 mix (both inputs are [DocIds,Tfs]), so the
	// dispatch MUST happen here on per-input Version -- reading a V0 gob section as
	// a V2 flat array (or vice versa) would write garbage LOSSLESSLY into the output
	// (a permanent B1-class corruption). Dispatching on per-input Version is the only
	// thing that prevents it.
	//
	// This run is built UNCONDITIONALLY and is the source of the persisted exact
	// per-doc length section on BOTH output paths: the V2 flat column (writeV2 on)
	// AND the V0 gob (writeV2 off, the default). The clamped uint16 run
	// (c.propertyLengthsToWrite, built above) feeds ONLY the block-impact
	// MaxImpactPropLength path (createBlocks via writeIndividualNode), which is the
	// WAND upper bound and is allowed -- required -- to be clamped. Sourcing the gob
	// from the clamped run was the M1 bug: a >65535-token doc was re-clamped to 65535
	// and persisted as the EXACT score length on the first V0->V0 compaction, deferring
	// the B1 corruption by exactly one compaction on the default path.
	olderRun, err := c.c1.segment.loadPropertyLengthRunForVersion()
	if err != nil {
		return errors.Wrap(err, "read older input property lengths (lossless)")
	}
	newerRun, err := c.c2.segment.loadPropertyLengthRunForVersion()
	if err != nil {
		return errors.Wrap(err, "read newer input property lengths (lossless)")
	}
	c.propLengthRunToWrite = mergePropertyLengthRunsV2(olderRun, newerRun)

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
	if err := c.writeIndices(kis); err != nil {
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

	invertedVersion := uint8(0)
	if c.writeV2 {
		invertedVersion = segmentindex.SegmentInvertedVersionV2
	}

	c.invertedHeader = &segmentindex.HeaderInverted{
		Version:               invertedVersion,
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

func (c *compactorInverted) combinePropertyLengths() (uint64, float64) {
	count := c.c1.segment.invertedData.avgPropertyLengthsCount + c.c2.segment.invertedData.avgPropertyLengthsCount
	average := 0.0
	if !math.IsNaN(c.c1.segment.invertedData.avgPropertyLengthsAvg) && !math.IsInf(c.c1.segment.invertedData.avgPropertyLengthsAvg, 0) && c.c1.segment.invertedData.avgPropertyLengthsCount > 0 {
		average += c.c1.segment.invertedData.avgPropertyLengthsAvg * (float64(c.c1.segment.invertedData.avgPropertyLengthsCount) / float64(count))
	}
	if !math.IsNaN(c.c2.segment.invertedData.avgPropertyLengthsAvg) && !math.IsInf(c.c2.segment.invertedData.avgPropertyLengthsAvg, 0) && c.c2.segment.invertedData.avgPropertyLengthsCount > 0 {
		average += c.c2.segment.invertedData.avgPropertyLengthsAvg * (float64(c.c2.segment.invertedData.avgPropertyLengthsCount) / float64(count))
	}

	if count == 0 {
		return 0, 0
	}
	return count, average
}

func (c *compactorInverted) writePropertyLengths() (int, error) {
	count, average := c.combinePropertyLengths()

	if c.writeV2 {
		// V2 flat-column section: encodePropertyLengthsV2 embeds the 24-byte
		// prefix, so the whole section is written in one go from the merged
		// lossless run. The docID run is already sorted by the two-pointer merge,
		// so no extra sort pass is needed and writePropertyLengthsV2 only asserts
		// the sorted invariant.
		//
		// c.offset is the authoritative BodyWriter byte position; do() captures
		// treeOffset := c.offset right after this returns and persists it as
		// Header.IndexStart. The V0 branch below advances c.offset by its full
		// section size, so the V2 branch MUST do the same -- otherwise IndexStart
		// points into the property-length section and the reopened disk tree
		// parses proplen bytes as a node header (DiskTree.Get panic on read).
		n, err := writePropertyLengthsV2(c.segmentFile.BodyWriter(), average, count, c.propLengthRunToWrite)
		if err != nil {
			return 0, err
		}
		c.offset += n
		return n, nil
	}

	// V0 gob section: encode the merged LOSSLESS uint32 run, NOT the clamped
	// block-impact map. The gob is the persisted exact per-doc length the score
	// read decodes back, so it must carry a >65535 doc verbatim; clamping here was
	// the M1 corruption. The block-impact MaxImpactPropLength (WAND) path keeps
	// consuming the clamped propertyLengthsToWrite map via writeIndividualNode.
	encoded, err := gobenc.Encode(propLengthRunToMap(c.propLengthRunToWrite))
	if err != nil {
		return 0, err
	}

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

func (c *compactorInverted) writeKeys(ctx context.Context) ([]segmentindex.Key, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset

	var kis []segmentindex.Key
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
	}.KeyIndexAndWriteTo(c.segmentFile.BodyWriter(), c.docIdEncoder, c.tfEncoder, c.k1, c.b, c.avgPropLen)
}

func (c *compactorInverted) writeIndices(keys []segmentindex.Key) error {
	indices := segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.secondaryIndexCount,
		AllocChecker:        c.allocChecker,
	}

	_, err := indices.WriteTo(c.segmentFile.BodyWriter())
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
	// Property lengths were already merged via the two-pointer merge in do();
	// nothing to copy here. This function only resolves the tombstone bitmap.
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
