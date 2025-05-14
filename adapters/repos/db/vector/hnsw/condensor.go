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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type MemoryCondensor struct {
	newLogFile *os.File
	newLog     *bufWriter
	logger     logrus.FieldLogger
}

func (c *MemoryCondensor) Do(fileName string) error {
	c.logger.WithField("action", "hnsw_condensing").Infof("start hnsw condensing")
	defer c.logger.WithField("action", "hnsw_condensing_complete").Infof("completed hnsw condensing")

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
		if res.CompressionPQData != nil {
			if err := c.AddPQCompression(*res.CompressionPQData); err != nil {
				return fmt.Errorf("write pq data: %w", err)
			}
		} else if res.CompressionSQData != nil {
			if err := c.AddSQCompression(*res.CompressionSQData); err != nil {
				return fmt.Errorf("write sq data: %w", err)
			}
		} else {
			return errors.Wrap(err, "unavailable compression data")
		}
	}
	if res.MuveraEnabled {
		if err := c.AddMuvera(*res.EncoderMuvera); err != nil {
			return fmt.Errorf("write muvera data: %w", err)
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
		// If the tombstone was later removed, consolidate the two operations into a noop
		if _, ok := res.TombstonesDeleted[ts]; ok {
			continue
		}

		if err := c.AddTombstone(ts); err != nil {
			return errors.Wrapf(err,
				"write tombstone for node %d to commit log", ts)
		}
	}

	for rmts := range res.TombstonesDeleted {
		// If the tombstone was added previously, consolidate the two operations into a noop
		if _, ok := res.Tombstones[rmts]; ok {
			continue
		}

		if err := c.RemoveTombstone(rmts); err != nil {
			return errors.Wrapf(err,
				"write removed tombstone for node %d to commit log", rmts)
		}
	}

	for nodesDeleted := range res.NodesDeleted {
		if err := c.DeleteNode(nodesDeleted); err != nil {
			return errors.Wrapf(err,
				"write deleted node %d to commit log", nodesDeleted)
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

const writeUint64Size = 8

func writeUint64(w io.Writer, in uint64) error {
	var b [writeUint64Size]byte
	binary.LittleEndian.PutUint64(b[:], in)
	_, err := w.Write(b[:])
	return err
}

const writeUint32Size = 4

func writeUint32(w io.Writer, in uint32) error {
	var b [writeUint32Size]byte
	binary.LittleEndian.PutUint32(b[:], in)
	_, err := w.Write(b[:])
	return err
}

const writeUint16Size = 2

func writeUint16(w io.Writer, in uint16) error {
	var b [writeUint16Size]byte
	binary.LittleEndian.PutUint16(b[:], in)
	_, err := w.Write(b[:])
	return err
}

const writeByteSize = 1

func writeByte(w io.Writer, in byte) error {
	var b [writeByteSize]byte
	b[0] = in
	_, err := w.Write(b[:])
	return err
}

const writeBoolSize = 1

func writeBool(w io.Writer, in bool) error {
	var b [writeBoolSize]byte
	if in {
		b[0] = 1
	}
	_, err := w.Write(b[:])
	return err
}

const writeCommitTypeSize = 1

func writeCommitType(w io.Writer, in HnswCommitType) error {
	var b [writeCommitTypeSize]byte
	b[0] = byte(in)
	_, err := w.Write(b[:])
	return err
}

func writeUint64Slice(w io.Writer, in []uint64) error {
	for _, v := range in {
		err := writeUint64(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddNode adds an empty node
func (c *MemoryCondensor) AddNode(node *vertex) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, AddNode))
	ec.Add(writeUint64(c.newLog, node.id))
	ec.Add(writeUint16(c.newLog, uint16(node.level)))

	return ec.ToError()
}

func (c *MemoryCondensor) DeleteNode(id uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, DeleteNode))
	ec.Add(writeUint64(c.newLog, id))
	return ec.ToError()
}

func (c *MemoryCondensor) SetLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, ReplaceLinksAtLevel))
	ec.Add(writeUint64(c.newLog, nodeid))
	ec.Add(writeUint16(c.newLog, uint16(level)))

	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		// TODO: investigate why we get such massive connections
		targetLength = math.MaxUint16
		c.logger.WithField("action", "condense_commit_log").
			WithField("original_length", len(targets)).
			WithField("maximum_length", targetLength).
			Warning("condensor length of connections would overflow uint16, cutting off")
	}
	ec.Add(writeUint16(c.newLog, uint16(targetLength)))
	ec.Add(writeUint64Slice(c.newLog, targets[:targetLength]))

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
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, AddLinkAtLevel))
	ec.Add(writeUint64(c.newLog, nodeid))
	ec.Add(writeUint16(c.newLog, uint16(level)))
	ec.Add(writeUint64(c.newLog, target))
	return ec.ToError()
}

func (c *MemoryCondensor) SetEntryPointWithMaxLayer(id uint64, level int) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, SetEntryPointMaxLevel))
	ec.Add(writeUint64(c.newLog, id))
	ec.Add(writeUint16(c.newLog, uint16(level)))
	return ec.ToError()
}

func (c *MemoryCondensor) AddTombstone(nodeid uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, AddTombstone))
	ec.Add(writeUint64(c.newLog, nodeid))
	return ec.ToError()
}

func (c *MemoryCondensor) RemoveTombstone(nodeid uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(c.newLog, RemoveTombstone))
	ec.Add(writeUint64(c.newLog, nodeid))
	return ec.ToError()
}

func (c *MemoryCondensor) AddPQCompression(data compressionhelpers.PQData) error {
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

func (c *MemoryCondensor) AddSQCompression(data compressionhelpers.SQData) error {
	toWrite := make([]byte, 11)
	toWrite[0] = byte(AddSQ)
	binary.LittleEndian.PutUint32(toWrite[1:], math.Float32bits(data.A))
	binary.LittleEndian.PutUint32(toWrite[5:], math.Float32bits(data.B))
	binary.LittleEndian.PutUint16(toWrite[9:], data.Dimensions)
	_, err := c.newLog.Write(toWrite)
	return err
}

func (c *MemoryCondensor) AddMuvera(data multivector.MuveraData) error {
	gSize := 4 * data.Repetitions * data.KSim * data.Dimensions
	dSize := 4 * data.Repetitions * data.DProjections * data.Dimensions
	var buf bytes.Buffer
	buf.Grow(21 + int(gSize) + int(dSize))

	buf.WriteByte(byte(AddMuvera))                             // 1
	binary.Write(&buf, binary.LittleEndian, data.KSim)         // 4
	binary.Write(&buf, binary.LittleEndian, data.NumClusters)  // 4
	binary.Write(&buf, binary.LittleEndian, data.Dimensions)   // 4
	binary.Write(&buf, binary.LittleEndian, data.DProjections) // 4
	binary.Write(&buf, binary.LittleEndian, data.Repetitions)  // 4

	i := 0
	for _, gaussian := range data.Gaussians {
		for _, cluster := range gaussian {
			for _, el := range cluster {
				binary.Write(&buf, binary.LittleEndian, math.Float32bits(el))
				i++
			}
		}
	}

	i = 0
	for _, matrix := range data.S {
		for _, vector := range matrix {
			for _, el := range vector {
				binary.Write(&buf, binary.LittleEndian, math.Float32bits(el))
				i++
			}
		}
	}

	_, err := c.newLog.Write(buf.Bytes())
	return err
}

func NewMemoryCondensor(logger logrus.FieldLogger) *MemoryCondensor {
	return &MemoryCondensor{logger: logger}
}
