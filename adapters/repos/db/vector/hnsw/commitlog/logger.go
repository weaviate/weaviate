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

package commitlog

import (
	"encoding/binary"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type Logger struct {
	file *os.File
	bufw *bufWriter

	checksumLogger ChecksumLogger
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
	AddSQ
)

func NewLogger(fileName string, checksumLogger ChecksumLogger) *Logger {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	return NewLoggerWithFile(file, checksumLogger)
}

func NewLoggerWithFile(file *os.File, checksumLogger ChecksumLogger) *Logger {
	return &Logger{
		file:           file,
		bufw:           NewWriter(file),
		checksumLogger: checksumLogger,
	}
}

func (l *Logger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	var toWrite [11]byte
	toWrite[0] = byte(SetEntryPointMaxLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) AddNode(id uint64, level int) error {
	var toWrite [11]byte
	toWrite[0] = byte(AddNode)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) AddPQCompression(data compressionhelpers.PQData) error {
	var toWrite [10]byte
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

	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	if err != nil {
		return err
	}

	for _, encoder := range data.Encoders {
		b := encoder.ExposeDataForRestore()

		_, err := l.bufw.Write(b)
		if err != nil {
			return err
		}

		_, err = l.checksumLogger.Checksum(b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Logger) AddSQCompression(data compressionhelpers.SQData) error {
	var toWrite [11]byte
	toWrite[0] = byte(AddSQ)
	binary.LittleEndian.PutUint32(toWrite[1:], math.Float32bits(data.A))
	binary.LittleEndian.PutUint32(toWrite[5:], math.Float32bits(data.B))
	binary.LittleEndian.PutUint16(toWrite[9:], data.Dimensions)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) AddLinkAtLevel(id uint64, level int, target uint64) error {
	var toWrite [19]byte
	toWrite[0] = byte(AddLinkAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], uint16(level))
	binary.LittleEndian.PutUint64(toWrite[11:19], target)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
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
	_, err := l.bufw.Write(toWrite)
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:13])
	if err != nil {
		return err
	}

	_, err = l.checksumLogger.Checksum(toWrite[13:])
	return err
}

// chunks links in increments of 8, so that we never have to allocate a dynamic
// []byte size which would be guaranteed to escape to the heap
func (l *Logger) ReplaceLinksAtLevel(id uint64, level int, targets []uint64) error {
	var headers [13]byte
	headers[0] = byte(ReplaceLinksAtLevel)
	binary.LittleEndian.PutUint64(headers[1:9], id)
	binary.LittleEndian.PutUint16(headers[9:11], uint16(level))
	binary.LittleEndian.PutUint16(headers[11:13], uint16(len(targets)))
	_, err := l.bufw.Write(headers[:])
	if err != nil {
		return errors.Wrap(err, "write headers")
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(headers[:])
	if err != nil {
		return errors.Wrap(err, "write headers checksum")
	}

	i := 0
	// chunks of 8
	var buf [64]byte
	for i < len(targets) {
		if i != 0 && i%8 == 0 {
			if _, err := l.bufw.Write(buf[:]); err != nil {
				return errors.Wrap(err, "write link chunk")
			}

			_, err = l.checksumLogger.Write(buf[:])
			if err != nil {
				return errors.Wrap(err, "write link chunk checksum")
			}
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

		if _, err := l.bufw.Write(buf[start:end]); err != nil {
			return errors.Wrap(err, "write link remainder")
		}

		_, err = l.checksumLogger.Write(buf[start:end])
		if err != nil {
			return errors.Wrap(err, "write link remainder checksum")
		}
	}

	_, err = l.checksumLogger.Checksum(nil)
	return err
}

func (l *Logger) AddTombstone(id uint64) error {
	var toWrite [9]byte
	toWrite[0] = byte(AddTombstone)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) RemoveTombstone(id uint64) error {
	var toWrite [9]byte
	toWrite[0] = byte(RemoveTombstone)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) ClearLinks(id uint64) error {
	var toWrite [9]byte
	toWrite[0] = byte(ClearLinks)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) ClearLinksAtLevel(id uint64, level uint16) error {
	var toWrite [11]byte
	toWrite[0] = byte(ClearLinksAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint16(toWrite[9:11], level)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) DeleteNode(id uint64) error {
	var toWrite [9]byte
	toWrite[0] = byte(DeleteNode)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) Reset() error {
	var toWrite [1]byte
	toWrite[0] = byte(ResetIndex)
	_, err := l.bufw.Write(toWrite[:])
	if err != nil {
		return err
	}

	l.checksumLogger.Reset()

	_, err = l.checksumLogger.Checksum(toWrite[:])
	return err
}

func (l *Logger) FileSize() (int64, error) {
	i, err := l.file.Stat()
	if err != nil {
		return -1, err
	}

	return i.Size(), nil
}

func (l *Logger) FileName() (string, error) {
	i, err := l.file.Stat()
	if err != nil {
		return "", err
	}

	return i.Name(), nil
}

func (l *Logger) Flush() error {
	ec := &errorcompounder.ErrorCompounder{}

	ec.Add(l.bufw.Flush())
	ec.Add(l.checksumLogger.Flush())

	return ec.ToError()
}

func (l *Logger) Close() error {
	ec := &errorcompounder.ErrorCompounder{}

	ec.Add(l.Flush())
	ec.Add(l.file.Close())
	ec.Add(l.checksumLogger.Close())

	return ec.ToError()
}
