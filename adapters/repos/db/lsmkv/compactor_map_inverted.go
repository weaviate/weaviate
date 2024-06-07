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
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

type compactorMapInverted struct {
	c1 *segmentCursorCollectionReusable
	c2 *segmentCursorInvertedReusable

	c1IsOlder bool

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

	tombstonesToWrite map[uint64]struct{}
	tombstonesToClean map[uint64]struct{}
	tombstonesCleaned map[uint64]struct{}

	keysLen uint64
}

func newCompactorMapInvertedCollection(w io.WriteSeeker,
	c1 *segmentCursorCollectionReusable, c2 *segmentCursorInvertedReusable, level, secondaryIndexCount uint16,
	scratchSpacePath string, cleanupTombstones bool, c1IsOlder bool,
) *compactorMapInverted {
	return &compactorMapInverted{
		c1:                  c1,
		c2:                  c2,
		w:                   w,
		bufw:                bufio.NewWriterSize(w, 256*1024),
		currentLevel:        level,
		cleanupTombstones:   cleanupTombstones,
		secondaryIndexCount: secondaryIndexCount,
		scratchSpacePath:    scratchSpacePath,
		c1IsOlder:           c1IsOlder,
	}
}

func (c *compactorMapInverted) do() error {
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

	err = c.writeTombstones(tombstones)
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

	var dataEnd uint64 = segmentindex.HeaderSize + 2 + 2 + 8 + uint64((len(tombstones)+1)*8)
	if len(kis) > 0 {
		dataEnd = uint64(kis[len(kis)-1].ValueEnd) + uint64((len(tombstones)+1)*8)
	}
	if err := c.writeHeader(c.currentLevel, 0, c.secondaryIndexCount,
		dataEnd); err != nil {
		return errors.Wrap(err, "write header")
	}

	if err := c.writeKeysLength(); err != nil {
		return errors.Wrap(err, "write keys length")
	}

	return nil
}

func (c *compactorMapInverted) init() error {
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
func (c *compactorMapInverted) writeKeysLength() error {
	if _, err := c.w.Seek(16+2+2, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning to write header")
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, c.keysLen)

	_, err := c.bufw.Write(buf)

	return err
}

func (c *compactorMapInverted) writeKeyValueLen() error {
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

func (c *compactorMapInverted) writeTombstones(tombstones []uint64) error {
	// TODO: right now, we don't deal with tombstones, so we'll just write a zero
	// to keep the format consistent
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(tombstones)))
	if _, err := c.bufw.Write(buf); err != nil {
		return err
	}

	for _, docId := range tombstones {
		binary.BigEndian.PutUint64(buf, docId)
		if _, err := c.bufw.Write(buf); err != nil {
			return err
		}
	}

	return nil
}

func (c *compactorMapInverted) writeKeys() ([]segmentindex.Key, []uint64, error) {
	key1, value1, _ := c.c1.first()
	key2, value2, _ := c.c2.first()

	// the (dummy) header was already written, this is our initial offset

	var kis []segmentindex.Key
	pairs := newReusableInvertedPairs()
	me := newInvertedEncoder()
	sim := newSortedInvertedMerger()

	var err error
	if c.c1IsOlder {
		c.tombstonesToClean, err = c.c2.segment.GetTombstones()
		if err != nil {
			return nil, nil, errors.Wrap(err, "get tombstones")
		}
		c.tombstonesCleaned = make(map[uint64]struct{}, len(c.tombstonesToClean))

	} else {
		c.tombstonesToWrite, err = c.c2.segment.GetTombstones()
		if err != nil {
			return nil, nil, errors.Wrap(err, "get tombstones")
		}
		c.tombstonesCleaned = make(map[uint64]struct{})

	}

	for {
		if key1 == nil && key2 == nil {
			break
		}
		if bytes.Equal(key1, key2) {
			pairs.ResizeLeft(len(value1))
			pairs.ResizeRight(len(value2))

			for i, v := range value1 {
				if err := pairs.left[i].FromBytesLegacy(v.value, false); err != nil {
					return nil, nil, err
				}
				pairs.left[i].Tombstone = v.tombstone
				docId := binary.BigEndian.Uint64(pairs.left[i].Key)

				if c.c1IsOlder {
					if _, ok := c.tombstonesToClean[docId]; ok {
						pairs.left[i].Tombstone = true
						c.tombstonesCleaned[docId] = struct{}{}
					} else if v.tombstone {
						c.tombstonesToWrite[docId] = struct{}{}
					}
				} else if !c.c1IsOlder && v.tombstone {
					c.tombstonesToClean[docId] = struct{}{}
				}
			}

			for i, v := range value2 {
				if err := pairs.right[i].FromBytes(v.value, false, c.c2.segment.invertedKeyLength, c.c2.segment.invertedValueLength); err != nil {
					return nil, nil, err
				}
			}
			if c.c1IsOlder {
				sim.reset([][]InvertedPair{pairs.left, pairs.right})
			} else {
				sim.reset([][]InvertedPair{pairs.right, pairs.left})
			}

			mergedPairs, err := sim.
				doKeepTombstonesReusable()
			if err != nil {
				return nil, nil, err
			}

			mergedEncoded, err := me.DoMultiReusable(mergedPairs)
			if err != nil {
				return nil, nil, err
			}

			if values, skip := c.cleanupValues(mergedEncoded); !skip {
				ki, err := c.writeIndividualNode(c.offset, key2, values)
				if err != nil {
					return nil, nil, errors.Wrap(err, "write individual node (equal keys)")
				}

				c.offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			// advance both!
			key1, value1, _ = c.c1.next()
			key2, value2, _ = c.c2.next()
			continue
		}

		if (key1 != nil && bytes.Compare(key1, key2) == -1) || key2 == nil {
			// key 1 is smaller
			if values, skip := c.cleanupValues(value1); !skip {
				ki, err := c.writeIndividualNodeLegacy(c.offset, key1, values)
				if err != nil {
					return nil, nil, errors.Wrap(err, "write individual node (key1 smaller)")
				}

				c.offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			key1, value1, _ = c.c1.next()
		} else {
			// key 2 is smaller
			if values, skip := c.cleanupValues(value2); !skip {
				ki, err := c.writeIndividualNode(c.offset, key2, values)
				if err != nil {
					return nil, nil, errors.Wrap(err, "write individual node (key2 smaller)")
				}

				c.offset = ki.ValueEnd
				kis = append(kis, ki)
			}
			key2, value2, _ = c.c2.next()
		}
	}
	tombstones := c.computeTombstones()
	return kis, tombstones, nil
}

func (c *compactorMapInverted) writeIndividualNode(offset int, key []byte,
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
	}.KeyIndexAndWriteTo(c.bufw)
}

func (c *compactorMapInverted) writeIndividualNodeLegacy(offset int, key []byte,
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

func (c *compactorMapInverted) writeIndices(keys []segmentindex.Key) error {
	indices := segmentindex.Indexes{
		Keys:                keys,
		SecondaryIndexCount: c.secondaryIndexCount,
		ScratchSpacePath:    c.scratchSpacePath,
	}

	_, err := indices.WriteTo(c.bufw)
	return err
}

// writeHeader assumes that everything has been written to the underlying
// writer and it is now safe to seek to the beginning and override the initial
// header
func (c *compactorMapInverted) writeHeader(level, version, secondaryIndices uint16,
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
func (c *compactorMapInverted) cleanupValues(values []value) (vals []value, skip bool) {
	if !c.cleanupTombstones {
		return values, false
	}

	// Reuse input slice not to allocate new memory
	// Rearrange slice in a way that tombstoned values are moved to the end
	// and reduce slice's length.
	last := 0
	for i := 0; i < len(values); i++ {
		docId := binary.BigEndian.Uint64(values[i].value[0:8])
		if _, ok := c.tombstonesToClean[docId]; ok {
			c.tombstonesCleaned[docId] = struct{}{}
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

func (c *compactorMapInverted) computeTombstones() []uint64 {
	tombstones := make([]uint64, 0, len(c.tombstonesToWrite)+len(c.tombstonesToClean)-len(c.tombstonesCleaned))

	for docId := range c.tombstonesToWrite {
		tombstones = append(tombstones, docId)
	}

	for docId := range c.tombstonesToClean {
		if _, ok := c.tombstonesCleaned[docId]; !ok {
			tombstones = append(tombstones, docId)
		}
	}

	return tombstones
}
