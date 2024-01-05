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

package hnsw

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type MemoryCondensor struct {
	newLogFile *os.File
	newLog     *bufWriter
	logger     logrus.FieldLogger
}

func (c *MemoryCondensor) Do(fileName string) error {
	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}
	defer fd.Close()
	fdBuf := bufio.NewReaderSize(fd, 256*1024)

	res, _, err := NewDeserializer(c.logger).Do(fdBuf, nil, true)
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

	if res.Compressed {
		if err := c.AddPQ(res.PQData); err != nil {
			return fmt.Errorf("write pq data: %w", err)
		}
	}

	for _, node := range res.Nodes {
		if node == nil {
			// nil nodes occur when we've grown, but not inserted anything yet
			continue
		}

		if node.level > 0 {
			// nodes are implicitly added when they are first linked, if the level is
			// not zero we know this node was new. If the level is zero it doesn't
			// matter if it gets added explicitly or implicitly
			if err := c.AddNode(node); err != nil {
				return errors.Wrapf(err, "write node %d to commit log", node.id)
			}
		}

		for level, links := range node.connections {
			if res.ReplaceLinks(node.id, uint16(level)) {
				if err := c.SetLinksAtLevel(node.id, level, links); err != nil {
					return errors.Wrapf(err,
						"write links for node %d at level %d to commit log", node.id, level)
				}
			} else {
				if err := c.AddLinksAtLevel(node.id, uint16(level), links); err != nil {
					return errors.Wrapf(err,
						"write links for node %d at level %d to commit log", node.id, level)
				}
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

func (c *MemoryCondensor) writeUint64(w *bufWriter, in uint64) error {
	toWrite := make([]byte, 8)
	binary.LittleEndian.PutUint64(toWrite[0:8], in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor) writeUint16(w *bufWriter, in uint16) error {
	toWrite := make([]byte, 2)
	binary.LittleEndian.PutUint16(toWrite[0:2], in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor) writeCommitType(w *bufWriter, in HnswCommitType) error {
	toWrite := make([]byte, 1)
	toWrite[0] = byte(in)
	_, err := w.Write(toWrite)
	if err != nil {
		return err
	}

	return nil
}

func (c *MemoryCondensor) writeUint64Slice(w *bufWriter, in []uint64) error {
	for _, v := range in {
		err := c.writeUint64(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddNode adds an empty node
func (c *MemoryCondensor) AddNode(node *vertex) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, AddNode))
	ec.Add(c.writeUint64(c.newLog, node.id))
	ec.Add(c.writeUint16(c.newLog, uint16(node.level)))

	return ec.ToError()
}

func (c *MemoryCondensor) SetLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, ReplaceLinksAtLevel))
	ec.Add(c.writeUint64(c.newLog, nodeid))
	ec.Add(c.writeUint16(c.newLog, uint16(level)))

	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		// TODO: investigate why we get such massive connections
		targetLength = math.MaxUint16
		c.logger.WithField("action", "condense_commit_log").
			WithField("original_length", len(targets)).
			WithField("maximum_length", targetLength).
			Warning("condensor length of connections would overflow uint16, cutting off")
	}
	ec.Add(c.writeUint16(c.newLog, uint16(targetLength)))
	ec.Add(c.writeUint64Slice(c.newLog, targets[:targetLength]))

	return ec.ToError()
}

func (c *MemoryCondensor) AddLinksAtLevel(nodeid uint64, level uint16, targets []uint64) error {
	toWrite := make([]byte, 13+len(targets)*8)
	toWrite[0] = byte(AddLinksAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], nodeid)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	binary.LittleEndian.PutUint16(toWrite[11:13], uint16(len(targets)))
	for i, target := range targets {
		offsetStart := 13 + i*8
		offsetEnd := offsetStart + 8
		binary.LittleEndian.PutUint64(toWrite[offsetStart:offsetEnd], target)
	}
	_, err := c.newLog.Write(toWrite)
	return err
}

func (c *MemoryCondensor) AddLinkAtLevel(nodeid uint64, level uint16, target uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, AddLinkAtLevel))
	ec.Add(c.writeUint64(c.newLog, nodeid))
	ec.Add(c.writeUint16(c.newLog, uint16(level)))
	ec.Add(c.writeUint64(c.newLog, target))

	return ec.ToError()
}

func (c *MemoryCondensor) SetEntryPointWithMaxLayer(id uint64, level int) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, SetEntryPointMaxLevel))
	ec.Add(c.writeUint64(c.newLog, id))
	ec.Add(c.writeUint16(c.newLog, uint16(level)))

	return ec.ToError()
}

func (c *MemoryCondensor) AddTombstone(nodeid uint64) error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.writeCommitType(c.newLog, AddTombstone))
	ec.Add(c.writeUint64(c.newLog, nodeid))

	return ec.ToError()
}

func (c *MemoryCondensor) AddPQ(data compressionhelpers.PQData) error {
	toWrite := make([]byte, 10)
	toWrite[0] = byte(AddPQ)
	binary.LittleEndian.PutUint16(toWrite[1:3], data.Dimensions)
	toWrite[3] = byte(data.EncoderType)
	binary.LittleEndian.PutUint16(toWrite[4:6], data.Ks)
	binary.LittleEndian.PutUint16(toWrite[6:8], data.M)
	toWrite[8] = data.EncoderDistribution
	if data.UseBitsEncoding {
		toWrite[9] = 1
	} else {
		toWrite[9] = 0
	}

	for _, encoder := range data.Encoders {
		toWrite = append(toWrite, encoder.ExposeDataForRestore()...)
	}
	_, err := c.newLog.Write(toWrite)
	return err
}

func NewMemoryCondensor(logger logrus.FieldLogger) *MemoryCondensor {
	return &MemoryCondensor{logger: logger}
}
