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
	"bytes"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/integrity"
)

type commitloggerParser struct {
	strategy string

	reader         io.Reader
	checksumReader integrity.ChecksumReader

	bufNode *bytes.Buffer

	memtable *Memtable
}

func newCommitLoggerParser(strategy string, reader io.Reader, memtable *Memtable,
) *commitloggerParser {
	return &commitloggerParser{
		strategy:       strategy,
		reader:         reader,
		checksumReader: integrity.NewCRC32Reader(reader),
		bufNode:        bytes.NewBuffer(nil),
		memtable:       memtable,
	}
}

func (p *commitloggerParser) Do() error {
	switch p.strategy {
	case StrategyReplace:
		return p.doReplace()
	case StrategyMapCollection, StrategySetCollection, StrategyInverted:
		return p.doCollection()
	case StrategyRoaringSet:
		return p.doRoaringSet()
	case StrategyRoaringSetRange:
		return p.doRoaringSetRange()
	default:
		return errors.Errorf("unknown strategy %s on commit log parse", p.strategy)
	}
}

func (p *commitloggerParser) doRecord() (r io.Reader, err error) {
	var nodeLen uint32
	err = binary.Read(p.checksumReader, binary.LittleEndian, &nodeLen)
	if err != nil {
		return nil, errors.Wrap(err, "read commit node length")
	}

	p.bufNode.Reset()

	io.CopyN(p.bufNode, p.checksumReader, int64(nodeLen))

	// read checksum directly from the reader
	var checksum [4]byte
	_, err = io.ReadFull(p.reader, checksum[:])
	if err != nil {
		return nil, errors.Wrap(err, "read commit checksum")
	}

	// validate checksum
	if !bytes.Equal(checksum[:], p.checksumReader.Hash()) {
		return nil, errors.Wrap(ErrInvalidChecksum, "read commit entry")
	}

	return p.bufNode, nil
}
