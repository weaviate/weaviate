//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bufio"
	"bytes"
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

	w                io.WriteSeeker
	bufw             *bufio.Writer
	scratchSpacePath string
}

func newCompactorReplace(w io.WriteSeeker,
	c1, c2 *segmentCursorReplace, level, secondaryIndexCount uint16,
	scratchSpacePath string) *compactorReplace {
	return &compactorReplace{
		c1:                  c1,
		c2:                  c2,
		w:                   w,
		bufw:                bufio.NewWriterSize(w, 256*1024),
		currentLevel:        level,
		secondaryIndexCount: secondaryIndexCount,
		scratchSpacePath:    scratchSpacePath,
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
		if res1.primaryKey == nil && res2.primaryKey == nil {
			break
		}
		if bytes.Equal(res1.primaryKey, res2.primaryKey) {
			ki, err := c.writeIndividualNode(offset, res2.primaryKey, res2.value,
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

		if (res1.primaryKey != nil && bytes.Compare(res1.primaryKey, res2.primaryKey) == -1) || res2.primaryKey == nil {
			// key 1 is smaller
			ki, err := c.writeIndividualNode(offset, res1.primaryKey, res1.value,
				res1.secondaryKeys, err1 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (res1.primaryKey smaller)")
			}

			offset = ki.valueEnd
			kis = append(kis, ki)
			res1, err1 = c.c1.nextWithAllKeys()
		} else {
			// key 2 is smaller
			ki, err := c.writeIndividualNode(offset, res2.primaryKey, res2.value,
				res2.secondaryKeys, err2 == Deleted)
			if err != nil {
				return nil, errors.Wrap(err, "write individual node (res2.primaryKey smaller)")
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
	segNode := segmentReplaceNode{
		offset:              offset,
		tombstone:           tombstone,
		value:               value,
		primaryKey:          key,
		secondaryIndexCount: c.secondaryIndexCount,
		secondaryKeys:       secondaryKeys,
	}

	return segNode.KeyIndexAndWriteTo(c.bufw)
}

func (c *compactorReplace) writeIndices(keys []keyIndex) error {
	indices := &segmentIndices{
		keys:                keys,
		secondaryIndexCount: c.secondaryIndexCount,
		scratchSpacePath:    c.scratchSpacePath,
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
