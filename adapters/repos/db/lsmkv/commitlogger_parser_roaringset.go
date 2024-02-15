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
	for {
		var commitType CommitType

		err := binary.Read(p.checksumReader, binary.LittleEndian, &commitType)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.Wrap(err, "read commit type")
		}

		if !CommitTypeRoaringSet.Is(commitType) {
			return errors.Errorf("found a %s commit on a roaringset bucket", commitType.String())
		}

		var version uint8

		err = binary.Read(p.checksumReader, binary.LittleEndian, &version)
		if err != nil {
			return errors.Wrap(err, "read commit version")
		}

		switch version {
		case 0:
			{
				err = p.parseRoaringSetNodeV0()
			}
		case 1:
			{
				err = p.parseRoaringSetNodeV1()
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

func (p *commitloggerParser) parseRoaringSetNodeV0() error {
	return p.parseRoaringSetNode(p.reader)
}

func (p *commitloggerParser) parseRoaringSetNodeV1() error {
	reader, err := p.doRecord()
	if err != nil {
		return err
	}

	return p.parseRoaringSetNode(reader)
}

func (p *commitloggerParser) parseRoaringSetNode(reader io.Reader) error {
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
	if err := p.memtable.roaringSetAddRemoveBitmaps(key, segment.Additions(), segment.Deletions()); err != nil {
		return errors.Wrap(err, "add/remove bitmaps")
	}

	return nil
}
