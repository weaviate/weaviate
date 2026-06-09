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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// Columnar WAL node format:
//
//	[8 bytes: docID] [1 byte: live (0=tombstone, 1=live)] [N bytes: values]

// CommitTypeColumnar is defined explicitly (not via iota) so a future commit
// type added to the main const block cannot silently collide with it.
const CommitTypeColumnar CommitType = 4

func (p *commitloggerParser) doColumnar() error {
	nodeCache := make(map[uint64]columnarWALNode)

	var errWhileParsing error
	for {
		if ok, err := p.doColumnarOnce(nodeCache); err != nil {
			errWhileParsing = err
			break
		} else if !ok {
			break
		}
	}

	// Replay into memtable — latest per docID wins (already handled by map).
	for _, node := range nodeCache {
		p.memtable.columnarPutRow(node.docID, node.valid, node.values)
	}

	return errWhileParsing
}

type columnarWALNode struct {
	docID  uint64
	valid  bool
	values []byte
}

func (p *commitloggerParser) doColumnarOnce(cache map[uint64]columnarWALNode) (bool, error) {
	var commitType CommitType
	err := binary.Read(p.checksumReader, binary.LittleEndian, &commitType)
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "read commit type")
	}
	if !CommitTypeColumnar.Is(commitType) {
		return false, fmt.Errorf("found a %s commit on a columnar bucket", commitType.String())
	}

	var version uint8
	err = binary.Read(p.checksumReader, binary.LittleEndian, &version)
	if err != nil {
		return false, errors.Wrap(err, "read commit version")
	}

	r, err := p.doRecord()
	if err != nil {
		return false, err
	}

	// Read docID
	var docID uint64
	if err := binary.Read(r, binary.LittleEndian, &docID); err != nil {
		return false, errors.Wrap(err, "read docID")
	}

	// Read valid flag
	var validByte uint8
	if err := binary.Read(r, binary.LittleEndian, &validByte); err != nil {
		return false, errors.Wrap(err, "read valid flag")
	}

	// Read remaining bytes as values
	values := make([]byte, p.bufNode.Len())
	copy(values, p.bufNode.Bytes())

	cache[docID] = columnarWALNode{
		docID:  docID,
		valid:  validByte != 0,
		values: values,
	}

	return true, nil
}

func isCommitLogCompatibleColumnar(wal io.ReadSeeker) (compatible bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			compatible = false
			if rerr, ok := r.(error); ok {
				err = rerr
			} else {
				err = fmt.Errorf("recover: %v", r)
			}
		}
	}()

	if _, err := wal.Seek(0, io.SeekStart); err != nil {
		return false, err
	}

	parser := newCommitLoggerParser(StrategyColumnar, wal, nil)
	cache := make(map[uint64]columnarWALNode)
	for range commitlogEntriesToCheck {
		if ok, err := parser.doColumnarOnce(cache); err != nil {
			return false, err
		} else if !ok {
			break
		}
	}
	return true, nil
}
