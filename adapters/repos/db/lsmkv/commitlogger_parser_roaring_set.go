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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/usecases/byteops"
)

func (p *commitloggerParser) doRoaringSet() error {
	prs := &commitlogParserRoaringSet{
		parser: p,
		consume: func(mt *Memtable, key []byte, additions, deletions []uint64) error {
			return mt.roaringSetAddRemoveSlices(key, additions, deletions)
		},
	}

	return prs.parse()
}

type commitlogParserRoaringSet struct {
	parser *commitloggerParser
	// consume takes the memtable per entry because a chunk boundary replaces it.
	// Binding a method value here would keep feeding the one already written out.
	consume func(mt *Memtable, key []byte, additions, deletions []uint64) error
}

func (prs *commitlogParserRoaringSet) parse() error {
	for {
		if ok, err := prs.parseOnce(); err != nil {
			return err
		} else if !ok {
			break
		}

		if err := prs.parser.cutChunkIfFull(prs.parser.memtable.Size()); err != nil {
			return err
		}
	}
	return nil
}

func (prs *commitlogParserRoaringSet) parseOnce() (ok bool, err error) {
	var commitType CommitType

	err = binary.Read(prs.parser.checksumReader, binary.LittleEndian, &commitType)
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "read commit type")
	}

	if !CommitTypeRoaringSet.Is(commitType) && !CommitTypeRoaringSetList.Is(commitType) {
		return false, errors.Errorf("found a %s commit on a roaringset bucket", commitType.String())
	}

	var version uint8

	err = binary.Read(prs.parser.checksumReader, binary.LittleEndian, &version)
	if err != nil {
		return false, errors.Wrap(err, "read commit version")
	}

	switch version {
	case 0:
		{
			err = prs.parseNodeV0()
		}
	case 1:
		{
			err = prs.parseNodeV1(commitType)
		}
	default:
		{
			return false, fmt.Errorf("unsupported commit version %d", version)
		}
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (prs *commitlogParserRoaringSet) parseNodeV0() error {
	return prs.parseNode(prs.parser.reader)
}

func (prs *commitlogParserRoaringSet) parseNodeV1(commitType CommitType) error {
	reader, err := prs.parser.doRecord()
	if err != nil {
		return err
	}
	if commitType == CommitTypeRoaringSet {
		return prs.parseNode(reader)
	} else {
		return prs.parseNodeList(reader)
	}
}

func (prs *commitlogParserRoaringSet) parseNode(reader io.Reader) error {
	segBuf, err := readSegmentRecord(reader)
	if err != nil {
		return err
	}

	segment := roaringset.NewSegmentNodeFromBuffer(segBuf)
	// PrimaryKey is a window into segBuf and roaringSetAddRemoveSlices keeps the
	// slice it is given, so an uncopied key holds the whole record.
	key := bytes.Clone(segment.PrimaryKey())

	if err := prs.consume(prs.parser.memtable, key,
		segment.Additions().ToArray(), segment.Deletions().ToArray()); err != nil {
		return fmt.Errorf("consume segment additions/deletions: %w", err)
	}

	return nil
}

func (prs *commitlogParserRoaringSet) parseNodeList(reader io.Reader) error {
	segBuf, err := readSegmentRecord(reader)
	if err != nil {
		return err
	}

	segment := roaringset.NewSegmentNodeListFromBuffer(segBuf)
	key := bytes.Clone(segment.PrimaryKey()) // copied for the reason parseNode gives
	if err := prs.consume(prs.parser.memtable, key,
		segment.Additions(), segment.Deletions()); err != nil {
		return errors.Wrap(err, "add/remove bitmaps")
	}

	return nil
}

// maxRecordPrealloc caps the buffer a record's length field sizes before any of
// the record is read. A v0 record has no checksum, so that length can be
// garbage; a longer record grows the buffer only as its bytes arrive.
const maxRecordPrealloc = 16 << 20

// readSegmentRecord reads one length-prefixed SegmentNode or SegmentNodeList and
// checks that the lengths inside it stay within it. Their decoders slice by
// those lengths unchecked, so a damaged record would otherwise panic and fail
// the whole bucket instead of ending the replay at this record.
func readSegmentRecord(reader io.Reader) ([]byte, error) {
	var lenBuf [8]byte
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		return nil, errors.Wrap(err, "read segment len")
	}
	segmentLen := binary.LittleEndian.Uint64(lenBuf[:])
	if segmentLen < 8 || segmentLen > math.MaxInt {
		return nil, fmt.Errorf("invalid segment len %d", segmentLen)
	}

	total := int(segmentLen)
	segBuf := make([]byte, 8, min(total, maxRecordPrealloc))
	copy(segBuf, lenBuf[:])
	for len(segBuf) < total {
		start := len(segBuf)
		chunk := min(total-start, maxRecordPrealloc)
		segBuf = slices.Grow(segBuf, chunk)[:start+chunk]
		if _, err := io.ReadFull(reader, segBuf[start:]); err != nil {
			return nil, errors.Wrap(err, "read segment contents")
		}
	}

	if err := checkSegmentLayout(segBuf); err != nil {
		return nil, fmt.Errorf("segment of len %d: %w", segmentLen, err)
	}
	return segBuf, nil
}

// checkSegmentLayout checks the layout SegmentNode and SegmentNodeList share
// after the total length: uint64-prefixed additions, uint64-prefixed deletions,
// uint32-prefixed key.
func checkSegmentLayout(segBuf []byte) error {
	rw := byteops.NewReadWriter(segBuf)
	rw.MoveBufferToAbsolutePosition(8)
	for _, field := range [2]string{"additions", "deletions"} {
		lenBytes, err := rw.ReadBytesFromBufferChecked(8)
		if err != nil {
			return fmt.Errorf("read %s len: %w", field, err)
		}
		if err := rw.SkipChecked(binary.LittleEndian.Uint64(lenBytes)); err != nil {
			return fmt.Errorf("read %s: %w", field, err)
		}
	}
	if _, err := rw.ReadBytesFromBufferWithUint32LengthIndicatorChecked(); err != nil {
		return fmt.Errorf("read key: %w", err)
	}
	return nil
}
