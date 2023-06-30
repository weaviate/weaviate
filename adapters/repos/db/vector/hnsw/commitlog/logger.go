//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package commitlog

import (
	"encoding/binary"
	"os"

	"github.com/weaviate/weaviate/adapters/repos/db/asyncwriter"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

type Logger struct {
	writer *asyncwriter.AsyncQueuedWriter
}

// TODO: these are duplicates with the hnsw package, unify them
type HnswCommitType uint8 // 256 options, plenty of room for future extensions

// TODO: these are duplicates with the hnsw package, unify them
const (
	AddNode HnswCommitType = iota
	SetEntryPointMaxLevel
	AddLinkAtLevel
	ReplaceLinksAtLevel
	AddTombstone
	RemoveTombstone
	ClearLinks
	DeleteNode
	ResetIndex
	ClearLinksAtLevel // added in v1.8.0-rc.1, see https://github.com/weaviate/weaviate/issues/1701
	AddLinksAtLevel   // added in v1.8.0-rc.1, see https://github.com/weaviate/weaviate/issues/1705
	AddPQ
)

func NewLogger(fileName string) *Logger {
	return &Logger{writer: asyncwriter.NewAsyncQueuedWriter(fileName)}
}

func NewLoggerWithFile(file *os.File) *Logger {
	return &Logger{writer: asyncwriter.NewAsyncQueuedWriterWithFile(file)}
}

func (l *Logger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	toWrite := make([]byte, 11)
	toWrite[0] = byte(SetEntryPointMaxLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))

	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) AddNode(id uint64, level int) error {
	toWrite := make([]byte, 11)
	toWrite[0] = byte(AddNode)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) AddPQ(data ssdhelpers.PQData) error {
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
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) AddLinkAtLevel(id uint64, level int, target uint64) error {
	toWrite := make([]byte, 19)
	toWrite[0] = byte(AddLinkAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	binary.LittleEndian.PutUint64(toWrite[11:19], target)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) AddLinksAtLevel(id uint64, level int, targets []uint64) error {
	toWrite := make([]byte, 13+len(targets)*8)
	toWrite[0] = byte(AddLinksAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	binary.LittleEndian.PutUint16(toWrite[11:13], uint16(len(targets)))
	for i, target := range targets {
		offsetStart := 13 + i*8
		offsetEnd := offsetStart + 8
		binary.LittleEndian.PutUint64(toWrite[offsetStart:offsetEnd], target)
	}
	l.writer.QueueData(toWrite)
	return nil
}

// chunks links in increments of 8, so that we never have to allocate a dynamic
// []byte size which would be guaranteed to escape to the heap
func (l *Logger) ReplaceLinksAtLevel(id uint64, level int, targets []uint64) error {
	headers := make([]byte, 13)
	headers[0] = byte(ReplaceLinksAtLevel)
	binary.LittleEndian.PutUint64(headers[1:9], id)
	binary.LittleEndian.PutUint16(headers[9:11], uint16(level))
	binary.LittleEndian.PutUint16(headers[11:13], uint16(len(targets)))
	//l.writer.QueueData(headers)
	toWrite := headers

	i := 0
	// chunks of 8
	buf := make([]byte, 64)
	for i < len(targets) {
		if i != 0 && i%8 == 0 {
			bufCopy := make([]byte, len(toWrite)+64)
			copy(bufCopy, toWrite)
			copy(bufCopy[len(toWrite):], buf)
			toWrite = bufCopy
			//l.writer.QueueData(buf)
		}

		pos := i % 8
		start := pos * 8
		end := start + 8
		binary.LittleEndian.PutUint64(buf[start:end], targets[i])

		i++
	}

	// remainder
	if i != 0 {
		start := 0
		end := i % 8 * 8
		if end == 0 {
			end = 64
		}
		bufCopy := make([]byte, len(toWrite)+end-start)
		copy(bufCopy, toWrite)
		copy(bufCopy[len(toWrite):], buf[start:end])
		toWrite = bufCopy
		//l.writer.QueueData(buf[start:end])
	}
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) AddTombstone(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(AddTombstone)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) RemoveTombstone(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(RemoveTombstone)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) ClearLinks(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(ClearLinks)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) ClearLinksAtLevel(id uint64, level uint16) error {
	toWrite := make([]byte, 11)
	toWrite[0] = byte(ClearLinksAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], level)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) DeleteNode(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(DeleteNode)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) Reset() error {
	toWrite := make([]byte, 1)
	toWrite[0] = byte(ResetIndex)
	l.writer.QueueData(toWrite)
	return nil
}

func (l *Logger) FileSize() (int64, error) {
	i, err := l.writer.Stat()
	if err != nil {
		return -1, err
	}

	return i.Size(), nil
}

func (l *Logger) FileName() (string, error) {
	i, err := l.writer.Stat()
	if err != nil {
		return "", err
	}

	return i.Name(), nil
}

func (l *Logger) Flush() error {
	return l.writer.Flush()
}

func (l *Logger) Close() error {
	return l.writer.Close()
}
