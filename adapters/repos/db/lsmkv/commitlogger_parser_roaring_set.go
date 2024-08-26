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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func (p *commitloggerParser) doRoaringSet() error {
	prs := &commitlogParserRoaringSet{
		parser:  p,
		consume: p.memtable.roaringSetAddRemoveSlices,
	}

	return prs.parse()
}

type commitlogParserRoaringSet struct {
	parser  *commitloggerParser
	consume func(key []byte, additions, deletions []uint64) error
}

func (prs *commitlogParserRoaringSet) parse() error {
	for {
		var commitType CommitType

		err := binary.Read(prs.parser.checksumReader, binary.LittleEndian, &commitType)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.Wrap(err, "read commit type")
		}

		if !CommitTypeRoaringSet.Is(commitType) && !CommitTypeRoaringSetList.Is(commitType) {
			return errors.Errorf("found a %s commit on a roaringset bucket", commitType.String())
		}

		var version uint8

		err = binary.Read(prs.parser.checksumReader, binary.LittleEndian, &version)
		if err != nil {
			return errors.Wrap(err, "read commit version")
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
				return fmt.Errorf("unsupported commit version %d", version)
			}
		}
		if err != nil {
			return err
		}
	}

	return nil
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
	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		return errors.Wrap(err, "read segment len")
	}
	segmentLen := binary.LittleEndian.Uint64(lenBuf)

	segBuf := make([]byte, segmentLen)
	copy(segBuf, lenBuf)
	if _, err := io.ReadFull(reader, segBuf[8:]); err != nil {
		return errors.Wrap(err, "read segment contents")
	}

	segment := roaringset.NewSegmentNodeFromBuffer(segBuf)
	key := segment.PrimaryKey()

	if err := prs.consume(key, segment.Additions().ToArray(), segment.Deletions().ToArray()); err != nil {
		return fmt.Errorf("consume segment additions/deletions: %w", err)
	}

	return nil
}

func (prs *commitlogParserRoaringSet) parseNodeList(reader io.Reader) error {
	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		return errors.Wrap(err, "read segment len")
	}
	segmentLen := binary.LittleEndian.Uint64(lenBuf)

	segBuf := make([]byte, segmentLen)
	copy(segBuf, lenBuf)
	if _, err := io.ReadFull(reader, segBuf[8:]); err != nil {
		return errors.Wrap(err, "read segment contents")
	}

	segment := roaringset.NewSegmentNodeListFromBuffer(segBuf)
	key := segment.PrimaryKey()
	if err := prs.consume(key, segment.Additions(), segment.Deletions()); err != nil {
		return errors.Wrap(err, "add/remove bitmaps")
	}

	return nil
}
