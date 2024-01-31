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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/diskio"
)

func (p *commitloggerParser) doRoaringSet() error {
	metered := diskio.NewMeteredReader(p.r, p.metrics.TrackStartupReadWALDiskIO)
	p.reader = bufio.NewReaderSize(metered, 1*1024*1024)

	for {
		var commitType CommitType

		err := binary.Read(p.reader, binary.LittleEndian, &commitType)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return errors.Wrap(err, "read commit type")
		}

		if CommitTypeRoaringSet.Is(commitType) {
			if err := p.parseRoaringSetNode(); err != nil {
				return errors.Wrap(err, "read collection node")
			}
		} else {
			return errors.Errorf("found a %s commit on collection bucket", commitType.String())
		}
	}

	return nil
}

func (p *commitloggerParser) parseRoaringSetNode() error {
	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(p.reader, lenBuf); err != nil {
		return errors.Wrap(err, "read segment len")
	}
	segmentLen := binary.LittleEndian.Uint64(lenBuf)

	segBuf := make([]byte, segmentLen)
	copy(segBuf, lenBuf)
	if _, err := io.ReadFull(p.reader, segBuf[8:]); err != nil {
		return errors.Wrap(err, "read segment contents")
	}

	segment := roaringset.NewSegmentNodeFromBuffer(segBuf)
	key := segment.PrimaryKey()
	if err := p.memtable.roaringSetAddRemoveBitmaps(key, segment.Additions(), segment.Deletions()); err != nil {
		return errors.Wrap(err, "add/remove bitmaps")
	}

	return nil
}
