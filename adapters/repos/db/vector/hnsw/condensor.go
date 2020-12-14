//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type MemoryCondensor struct {
	newLog *os.File
	logger logrus.FieldLogger
}

func (c *MemoryCondensor) Do(fileName string) error {
	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}

	res, err := NewDeserializer(c.logger).Do(fd, nil)
	if err != nil {
		return errors.Wrap(err, "read commit log to be condensed")
	}

	newLog, err := os.OpenFile(fmt.Sprintf("%s.condensed", fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return errors.Wrap(err, "open new commit log file for writing")
	}

	c.newLog = newLog

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

	if err := c.newLog.Close(); err != nil {
		return errors.Wrap(err, "close new commit log")
	}

	if err := os.Remove(fileName); err != nil {
		return errors.Wrap(err, "cleanup old (uncondensed) commit log")
	}

	return nil
}

func (c *MemoryCondensor) writeUint64(w io.Writer, in uint64) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing uint64")
	}

	return nil
}

func (c *MemoryCondensor) writeUint16(w io.Writer, in uint16) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing uint16")
	}

	return nil
}

func (c *MemoryCondensor) writeCommitType(w io.Writer, in HnswCommitType) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing commit type")
	}

	return nil
}

func (c *MemoryCondensor) writeUint64Slice(w io.Writer, in []uint64) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return errors.Wrap(err, "writing []uint64")
	}

	return nil
}

// AddNode adds an empty node
func (c *MemoryCondensor) AddNode(node *vertex) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, AddNode))
	ec.add(c.writeUint64(c.newLog, node.id))
	ec.add(c.writeUint16(c.newLog, uint16(node.level)))

	return ec.toError()
}

func (c *MemoryCondensor) SetLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
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

func (c *MemoryCondensor) SetEntryPointWithMaxLayer(id uint64, level int) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, SetEntryPointMaxLevel))
	ec.add(c.writeUint64(c.newLog, id))
	ec.add(c.writeUint16(c.newLog, uint16(level)))

	return ec.toError()
}

func (c *MemoryCondensor) AddTombstone(nodeid uint64) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, AddTombstone))
	ec.add(c.writeUint64(c.newLog, nodeid))

	return ec.toError()
}

func NewMemoryCondensor(logger logrus.FieldLogger) *MemoryCondensor {
	return &MemoryCondensor{logger: logger}
}
