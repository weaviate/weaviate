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
	"os"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type commitLogger struct {
	file   *os.File
	writer *bufio.Writer
	n      atomic.Int64
	path   string

	// e.g. when recovering from an existing log, we do not want to write into a
	// new log again
	paused bool
}

type CommitType uint16

const (
	CommitTypeReplace CommitType = iota // replace strategy

	// collection strategy - this can handle all cases as updates and deletes are
	// only appends in a collection strategy
	CommitTypeCollection
	CommitTypeRoaringSet
)

func (ct CommitType) String() string {
	switch ct {
	case CommitTypeReplace:
		return "replace"
	case CommitTypeCollection:
		return "collection"
	case CommitTypeRoaringSet:
		return "roaringset"
	default:
		return "unknown"
	}
}

func (ct CommitType) Is(checkedCommitType CommitType) bool {
	return ct == checkedCommitType
}

func newCommitLogger(path string) (*commitLogger, error) {
	out := &commitLogger{
		path: path + ".wal",
	}

	f, err := os.Create(out.path)
	if err != nil {
		return nil, err
	}

	out.file = f

	out.writer = bufio.NewWriter(f)
	return out, nil
}

func (cl *commitLogger) put(node segmentReplaceNode) error {
	if cl.paused {
		return nil
	}

	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.writer, binary.LittleEndian, CommitTypeReplace); err != nil {
		return err
	}
	n := 1

	if ki, err := node.KeyIndexAndWriteTo(cl.writer); err != nil {
		return err
	} else {
		n += ki.ValueEnd - ki.ValueStart
	}

	cl.n.Add(int64(n))

	return nil
}

func (cl *commitLogger) append(node segmentCollectionNode) error {
	if cl.paused {
		return nil
	}

	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.writer, binary.LittleEndian, CommitTypeCollection); err != nil {
		return err
	}
	n := 1

	if ki, err := node.KeyIndexAndWriteTo(cl.writer); err != nil {
		return err
	} else {
		n += ki.ValueEnd - ki.ValueStart
	}

	cl.n.Add(int64(n))

	return nil
}

func (cl *commitLogger) add(node *roaringset.SegmentNode) error {
	if cl.paused {
		return nil
	}

	if err := binary.Write(cl.writer, binary.LittleEndian, CommitTypeRoaringSet); err != nil {
		return err
	}
	n := 1

	if ki, err := node.KeyIndexAndWriteTo(cl.writer, 0); err != nil {
		return err
	} else {
		n += ki.ValueEnd - ki.ValueStart
	}

	cl.n.Add(int64(n))

	return nil
}

// Size returns the amount of data that has been written since the commit
// logger was initialized. After a flush a new logger is initialized which
// automatically resets the logger.
func (cl *commitLogger) Size() int64 {
	return cl.n.Load()
}

func (cl *commitLogger) close() error {
	if cl.paused {
		return errors.Errorf("attempting to close a paused commit logger")
	}

	if err := cl.writer.Flush(); err != nil {
		return err
	}

	return cl.file.Close()
}

func (cl *commitLogger) pause() {
	cl.paused = true
}

func (cl *commitLogger) unpause() {
	cl.paused = false
}

func (cl *commitLogger) delete() error {
	return os.Remove(cl.path)
}

func (cl *commitLogger) flushBuffers() error {
	return cl.writer.Flush()
}
