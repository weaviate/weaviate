package hnsw

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func newHnswCommitLogger(name string) *hnswCommitLogger {
	l := &hnswCommitLogger{
		events: make(chan []byte),
	}

	// TODO: adjust file path
	fd, err := os.OpenFile(fmt.Sprintf("./data/hnsw_commit_log_%s", name), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	l.logFile = fd

	l.StartLogging()
	return l
}

type hnswCommitLogger struct {
	events  chan []byte
	logFile *os.File
}

type hnswCommitType uint8 // 256 options, plenty of room for future extensions

const (
	addNode hnswCommitType = iota
	setEntryPointMaxLevel
	addLinkAtLevel
	replaceLinksAtLevel
)

// AddNode adds an empty node
func (l *hnswCommitLogger) AddNode(node *hnswVertex) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, addNode)
	l.writeUint32(w, uint32(node.id))
	l.writeUint16(w, uint16(node.level))

	l.events <- w.Bytes()

	return nil
}

func (l *hnswCommitLogger) SetEntryPointWithMaxLayer(id int, level int) error {

	w := &bytes.Buffer{}
	l.writeCommitType(w, setEntryPointMaxLevel)
	l.writeUint32(w, uint32(id))
	l.writeUint16(w, uint16(level))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) AddLinkAtLevel(nodeid int, level int, target uint32) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, addLinkAtLevel)
	l.writeUint32(w, uint32(nodeid))
	l.writeUint16(w, uint16(level))
	l.writeUint32(w, target)

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) ReplaceLinksAtLevel(nodeid int, level int, targets []uint32) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, replaceLinksAtLevel)
	l.writeUint32(w, uint32(nodeid))
	l.writeUint16(w, uint16(level))
	l.writeUint16(w, uint16(len(targets)))
	l.writeUint32Slice(w, targets)

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) StartLogging() {
	go func() {
		for {
			event := <-l.events
			l.logFile.Write(event)
		}
	}()
}

func (l *hnswCommitLogger) writeUint32(w io.Writer, in uint32) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing uint32: %v", err)
	}

	return nil
}

func (l *hnswCommitLogger) writeUint16(w io.Writer, in uint16) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing uint16: %v", err)
	}

	return nil
}

func (l *hnswCommitLogger) writeCommitType(w io.Writer, in hnswCommitType) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing commit type: %v", err)
	}

	return nil
}

func (l *hnswCommitLogger) writeUint32Slice(w io.Writer, in []uint32) error {
	err := binary.Write(w, binary.LittleEndian, &in)
	if err != nil {
		return fmt.Errorf("writing []uint32: %v", err)
	}

	return nil
}
