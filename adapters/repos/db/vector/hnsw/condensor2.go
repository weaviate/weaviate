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

package hnsw

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type MemoryCondensor2 struct {
	newLogFile *os.File
	newLog     *bufWriter
	logger     logrus.FieldLogger
}

func (c *MemoryCondensor2) Do(fileName string) error {
	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}
	fdBuf := bufio.NewReaderSize(fd, 256*1024)

	res, err := NewDeserializer(c.logger).Do(fdBuf, nil)
	if err != nil {
		return errors.Wrap(err, "read commit log to be condensed")
	}

	newLogFile, err := os.OpenFile(fmt.Sprintf("%s.condensed", fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrap(err, "open new commit log file for writing")
	}

	c.newLogFile = newLogFile

	c.newLog = NewWriterSize(c.newLogFile, 1*1024*1024)

	for _, node := range res.Nodes {
		if node == nil {
			// nil nodes occur when we've grown, but not inserted anything yet
			continue
		}

		if err := c.AddNode(node); err != nil {
			return errors.Wrapf(err, "write node %d to commit log", node.id)
		}

		for level, links := range node.connections {
			if err := c.SetLinksAtLevel(node.id, level, links); err != nil {
				return errors.Wrapf(err,
					"write links for node %d at level %dto commit log", node.id, level)
			}
		}
	}

	if res.EntrypointChanged {
		if err := c.SetEntryPointWithMaxLayer(res.Entrypoint,
			int(res.Level)); err != nil {
			return errors.Wrap(err, "write entrypoint to commit log")
		}
	}

	for ts := range res.Tombstones {
		if err := c.AddTombstone(ts); err != nil {
			return errors.Wrapf(err,
				"write tombstone for node %d to commit log", ts)
		}
	}

	if err := c.newLog.Flush(); err != nil {
		return errors.Wrap(err, "close new commit log")
	}

	if err := c.newLogFile.Close(); err != nil {
		return errors.Wrap(err, "close new commit log")
	}

	if err := os.Remove(fileName); err != nil {
		return errors.Wrap(err, "cleanup old (uncondensed) commit log")
	}

	return nil
}

func (c *MemoryCondensor2) writeUint64(w *bufWriter, in uint64) error {
	toWrite := make([]byte, 8)
	binary.LittleEndian.PutUint64(toWrite[0:8], in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor2) writeUint16(w *bufWriter, in uint16) error {
	toWrite := make([]byte, 2)
	binary.LittleEndian.PutUint16(toWrite, in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor2) writeCommitType(w *bufWriter, in HnswCommitType) error {
	toWrite := make([]byte, 1)
	toWrite[0] = byte(in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor2) writeUint64Slice(w *bufWriter, in []uint64) error {
	buf := make([]byte, 8*len(in))
	i := 0
	for i < len(in){
		if i != 0 && i%8 == 0 {
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}

		pos := i%8
		start := pos * 8
		end := start + 8
		binary.LittleEndian.PutUint64(buf[start:end], in[i])
		i++
	}

	if i != 0 {
		start := 0
		end := i % 8 * 8
		if end == 0 {
			end = 64
		}

		if _, err := w.Write(buf[start:end]); err != nil {
			return err
		}
	}

	return nil
}

// AddNode adds an empty node
func (c *MemoryCondensor2) AddNode(node *vertex) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, AddNode))
	ec.add(c.writeUint64(c.newLog, node.id))
	ec.add(c.writeUint16(c.newLog, uint16(node.level)))

	return ec.toError()
}

func (c *MemoryCondensor2) SetLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, ReplaceLinksAtLevel))
	ec.add(c.writeUint64(c.newLog, nodeid))
	ec.add(c.writeUint16(c.newLog, uint16(level)))

	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		// TODO: investigate why we get such massive connections
		targetLength = math.MaxUint16
		c.logger.WithField("action", "condense_commit_log").
			WithField("original_length", len(targets)).
			WithField("maximum_length", targetLength).
			Warning("condensor length of connections would overflow uint16, cutting off")
	}
	ec.add(c.writeUint16(c.newLog, uint16(targetLength)))
	ec.add(c.writeUint64Slice(c.newLog, targets[:targetLength]))

	return ec.toError()
}

func (c *MemoryCondensor2) SetEntryPointWithMaxLayer(id uint64, level int) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, SetEntryPointMaxLevel))
	ec.add(c.writeUint64(c.newLog, id))
	ec.add(c.writeUint16(c.newLog, uint16(level)))

	return ec.toError()
}

func (c *MemoryCondensor2) AddTombstone(nodeid uint64) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, AddTombstone))
	ec.add(c.writeUint64(c.newLog, nodeid))

	return ec.toError()
}

func NewMemoryCondensor2(logger logrus.FieldLogger) *MemoryCondensor2 {
	return &MemoryCondensor2{logger: logger}
}
