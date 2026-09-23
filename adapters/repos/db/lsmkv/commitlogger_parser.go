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

	// chunkWriteErr is a failure to write a chunk to disk, which Do returns the way it
	// returns a read failure. Recovery unlinks a WAL it cannot read any further, but
	// a WAL whose chunk never reached the disk is still the only copy of it.
	chunkWriteErr error

	// memtableRejectErr is an entry the memtable refused. The memtable is then not a faithful
	// image of the WAL, so recoverFromWAL writes it out as a segment rather than
	// adopting it as b.active. Only doReplace fills it.
	memtableRejectErr error

	// refusedEntries counts every entry memtableRejectErr stands for, across every chunk,
	// because the log line otherwise reports one of an unbounded number.
	refusedEntries int
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

// chunkedReplay cuts on the bytes held so far and on the WAL bytes read, because
// neither covers every strategy. A roaringsetrange memtable counts entries changed
// rather than bytes, and a roaring-set one can hold several times the WAL bytes behind it.
type chunkedReplay struct {
	memtableThreshold uint64
	walThreshold      int64
	readWALBytes      func() int64
	writeChunk        func(full *Memtable) (*Memtable, error)
}

// setChunkedReplay makes the next Do cut chunks. Do leaves the tail in
// p.memtable for the caller to write out.
func (p *commitloggerParser) setChunkedReplay(c chunkedReplay) {
	p.chunking = &c
}

// chunkIsFull takes heldBytes rather than reading the memtable, because the
// replace strategy carries the entries read so far in its deduplication cache.
func (p *commitloggerParser) chunkIsFull(heldBytes uint64) bool {
	if p.chunking == nil {
		return false
	}

	return heldBytes >= p.chunking.memtableThreshold ||
		p.chunking.readWALBytes()-p.chunkStart >= p.chunking.walThreshold
}

// cutChunkIfFull may only be called between entries. A record split across two
// segments cannot be read back. It takes heldBytes for the same reason chunkIsFull
// does, so no caller can measure the wrong thing by leaving it out.
func (p *commitloggerParser) cutChunkIfFull(heldBytes uint64) error {
	if !p.chunkIsFull(heldBytes) {
		return nil
	}

	return p.cutChunk()
}

// cutChunk writes the current memtable out and starts a fresh one. doReplace stores
// its deduplication cache between testing and cutting, so it cuts through this:
// cutChunkIfFull would test the freshly emptied cache and miss the held-bytes cut.
func (p *commitloggerParser) cutChunk() error {
	if p.chunking == nil {
		return nil
	}

	next, err := p.chunking.writeChunk(p.memtable)
	if err != nil {
		p.chunkWriteErr = err
		return err
	}

	p.memtable = next
	p.chunkStart = p.chunking.readWALBytes()

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
