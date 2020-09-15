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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// TODO: adjust file path, it needs to contain timestamps. Possibly use a
// directory as helpers
func commitLogFileName(rootPath, name string) string {
	return fmt.Sprintf("%s/%s.hnsw.commitlog", rootPath, name)
}

func NewCommitLogger(rootPath, name string) *hnswCommitLogger {
	l := &hnswCommitLogger{
		events: make(chan []byte),
	}

	fd, err := os.OpenFile(commitLogFileName(rootPath, name), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
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
	addTombstone
	removeTombstone
	clearLinks
	deleteNode
	resetIndex
)

// AddNode adds an empty node
func (l *hnswCommitLogger) AddNode(node *vertex) error {
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

func (l *hnswCommitLogger) AddTombstone(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, addTombstone)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) RemoveTombstone(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, removeTombstone)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) ClearLinks(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, clearLinks)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) DeleteNode(nodeid int) error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, deleteNode)
	l.writeUint32(w, uint32(nodeid))

	l.events <- w.Bytes()
	return nil
}

func (l *hnswCommitLogger) Reset() error {
	w := &bytes.Buffer{}
	l.writeCommitType(w, resetIndex)

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
