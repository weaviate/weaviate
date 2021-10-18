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
	"encoding/binary"
	"os"

	"github.com/pkg/errors"
)

type commitLogger struct {
	file   *os.File
	writer *bufio.Writer
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
)

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

	if _, err := node.KeyIndexAndWriteTo(cl.writer); err != nil {
		return err
	}

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

	if _, err := node.KeyIndexAndWriteTo(cl.writer); err != nil {
		return err
	}

	return nil
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
