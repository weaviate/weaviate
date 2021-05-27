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
}

type CommitType uint16

const (
	CommitTypePut          CommitType = iota // replace strategy
	CommitTypeSetTombstone                   // replace strategy

	// collection strategy - this can handle all cases as updates and deletes are
	// only appends in a collection strategy
	CommitTypeAppend
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

func (cl *commitLogger) put(key, value []byte) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.writer, binary.LittleEndian, CommitTypePut); err != nil {
		return err
	}

	keyLen := uint32(len(key))
	if err := binary.Write(cl.writer, binary.LittleEndian, &keyLen); err != nil {
		return err
	}

	if _, err := cl.writer.Write(key); err != nil {
		return err
	}

	valueLen := uint32(len(value))
	if err := binary.Write(cl.writer, binary.LittleEndian, &valueLen); err != nil {
		return err
	}

	if _, err := cl.writer.Write(value); err != nil {
		return err
	}

	return nil
}

func (cl *commitLogger) setTombstone(key []byte) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.writer, binary.LittleEndian, CommitTypeSetTombstone); err != nil {
		return err
	}

	keyLen := uint32(len(key))
	if err := binary.Write(cl.writer, binary.LittleEndian, &keyLen); err != nil {
		return err
	}

	if _, err := cl.writer.Write(key); err != nil {
		return err
	}

	return nil
}

func (cl *commitLogger) append(key []byte, values []value) error {
	// TODO: do we need a timestamp? if so, does it need to be a vector clock?
	if err := binary.Write(cl.writer, binary.LittleEndian, CommitTypeAppend); err != nil {
		return err
	}

	keyLen := uint32(len(key))
	if err := binary.Write(cl.writer, binary.LittleEndian, &keyLen); err != nil {
		return err
	}

	if _, err := cl.writer.Write(key); err != nil {
		return err
	}

	valuesLen := uint64(len(values))
	if err := binary.Write(cl.writer, binary.LittleEndian, &valuesLen); err != nil {
		return err
	}

	for _, value := range values {
		if err := binary.Write(cl.writer, binary.LittleEndian, value.tombstone); err != nil {
			return errors.Wrap(err, "write tombstone for value")
		}

		valueLen := uint64(len(value.value))
		if err := binary.Write(cl.writer, binary.LittleEndian, valueLen); err != nil {
			return errors.Wrap(err, "write len of value")
		}

		_, err := cl.writer.Write(value.value)
		if err != nil {
			return errors.Wrap(err, "write value")
		}
	}

	return nil
}

func (cl *commitLogger) close() error {
	if err := cl.writer.Flush(); err != nil {
		return err
	}

	return cl.file.Close()
}

func (cl *commitLogger) delete() error {
	return os.Remove(cl.path)
}
