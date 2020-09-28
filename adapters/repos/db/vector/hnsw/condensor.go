package hnsw

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

type MemoryCondensor struct {
	newLog *os.File
}

func (c *MemoryCondensor) Do(fileName string) error {
	fd, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "open commit log to be condensed")
	}

	res, err := NewDeserializer().Do(fd, nil)
	if err != nil {
		return errors.Wrap(err, "read commit log to be condensed")
	}

	newLog, err := os.OpenFile(fmt.Sprintf("%s.condensed", fileName),
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return errors.Wrap(err, "open new commit log file for writing")
	}

	c.newLog = newLog

	if err := c.SetEntryPointWithMaxLayer(int(res.entrypoint),
		int(res.level)); err != nil {
		return errors.Wrap(err, "write entrypoint to commit log")
	}

	for _, node := range res.nodes {
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

	for ts := range res.tombstones {
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

func (c *MemoryCondensor) writeUint32(w io.Writer, in uint32) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing uint32: %v", err)
	}

	return nil
}

func (c *MemoryCondensor) writeUint16(w io.Writer, in uint16) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing uint16: %v", err)
	}

	return nil
}

func (c *MemoryCondensor) writeCommitType(w io.Writer, in hnswCommitType) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing commit type: %v", err)
	}

	return nil
}

func (c *MemoryCondensor) writeUint32Slice(w io.Writer, in []uint32) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing []uint32: %v", err)
	}

	return nil
}

// AddNode adds an empty node
func (c *MemoryCondensor) AddNode(node *vertex) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, addNode))
	ec.add(c.writeUint32(c.newLog, uint32(node.id)))
	ec.add(c.writeUint16(c.newLog, uint16(node.level)))

	return ec.toError()
}

func (c *MemoryCondensor) SetLinksAtLevel(nodeid int, level int, targets []uint32) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, replaceLinksAtLevel))
	ec.add(c.writeUint32(c.newLog, uint32(nodeid)))
	ec.add(c.writeUint16(c.newLog, uint16(level)))
	ec.add(c.writeUint16(c.newLog, uint16(len(targets))))
	ec.add(c.writeUint32Slice(c.newLog, targets))

	return ec.toError()
}

func (c *MemoryCondensor) SetEntryPointWithMaxLayer(id int, level int) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, setEntryPointMaxLevel))
	ec.add(c.writeUint32(c.newLog, uint32(id)))
	ec.add(c.writeUint16(c.newLog, uint16(level)))

	return ec.toError()
}

func (c *MemoryCondensor) AddTombstone(nodeid int) error {
	ec := &errorCompounder{}
	ec.add(c.writeCommitType(c.newLog, addTombstone))
	ec.add(c.writeUint32(c.newLog, uint32(nodeid)))

	return ec.toError()
}

func NewMemoryCondensor() *MemoryCondensor {
	return &MemoryCondensor{}
}
