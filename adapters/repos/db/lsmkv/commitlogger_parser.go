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

	chunking   *chunkedReplay
	chunkStart int64

	// writeErr is a failure to write a chunk to disk, which Do returns the way it
	// returns a read failure. Recovery unlinks a WAL it cannot read any further, but
	// a WAL whose chunk never reached the disk is still the only copy of it.
	writeErr error
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

// chunkedReplay cuts on flushAndSwitchIfThresholdsMet's two size triggers, so a
// replay lands where the flush cycle would have. Neither alone covers every
// strategy. A roaringsetrange memtable reports entries changed rather than bytes,
// and a roaring-set one several times the bytes of the records behind it.
type chunkedReplay struct {
	memtableThreshold uint64
	walThreshold      int64
	walBytesRead      func() int64
	writeChunk        func(full *Memtable) (*Memtable, error)
}

// replayInChunks leaves the tail in p.memtable for the caller to write out.
func (p *commitloggerParser) replayInChunks(c chunkedReplay) {
	p.chunking = &c
}

// chunkFull takes held rather than reading the memtable, because the replace
// strategy carries the entries read so far in its deduplication cache.
func (p *commitloggerParser) chunkFull(held uint64) bool {
	if p.chunking == nil {
		return false
	}

	return held >= p.chunking.memtableThreshold ||
		p.chunking.walBytesRead()-p.chunkStart >= p.chunking.walThreshold
}

// chunkIfFull may only be called between entries. A record split across two
// segments cannot be read back.
func (p *commitloggerParser) chunkIfFull() error {
	if !p.chunkFull(p.memtable.Size()) {
		return nil
	}

	next, err := p.chunking.writeChunk(p.memtable)
	if err != nil {
		p.writeErr = err
		return err
	}

	p.memtable = next
	p.chunkStart = p.chunking.walBytesRead()

	return nil
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
