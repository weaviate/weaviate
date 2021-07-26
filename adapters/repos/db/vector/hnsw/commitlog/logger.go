package commitlog

import (
	"encoding/binary"
	"os"

	"github.com/pkg/errors"
)

type Logger struct {
	file *os.File
	bufw *bufWriter
}

type HnswCommitType uint8 // 256 options, plenty of room for future extensions

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
)

func NewLogger(fileName string) *Logger {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}

	return &Logger{file: file, bufw: NewWriter(file)}
}

func (l *Logger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	toWrite := make([]byte, 17)
	toWrite[0] = byte(SetEntryPointMaxLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint64(toWrite[9:17], uint64(level))
	_, err := l.bufw.Write(toWrite)
	return err
}

func (l *Logger) AddNode(id uint64, level int) error {
	toWrite := make([]byte, 17)
	toWrite[0] = byte(AddNode)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint64(toWrite[9:17], uint64(level))
	_, err := l.bufw.Write(toWrite)
	return err
}

func (l *Logger) AddLinkAtLevel(id uint64, level int, target uint64) error {
	toWrite := make([]byte, 25)
	toWrite[0] = byte(AddLinkAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	binary.LittleEndian.PutUint64(toWrite[9:17], uint64(level))
	binary.LittleEndian.PutUint64(toWrite[17:25], target)
	_, err := l.bufw.Write(toWrite)
	return err
}

// chunks links in increments of 8, so that we never have to allocate a dynamic
// []byte size which would be guaranteed to escape to the heap
func (l *Logger) ReplaceLinksAtLevel(id uint64, level int, targets []uint64) error {
	headers := make([]byte, 19)
	headers[0] = byte(ReplaceLinksAtLevel)
	binary.LittleEndian.PutUint64(headers[1:9], id)
	binary.LittleEndian.PutUint64(headers[9:17], uint64(level))
	binary.LittleEndian.PutUint16(headers[17:19], uint16(len(targets)))
	_, err := l.bufw.Write(headers)
	if err != nil {
		return errors.Wrap(err, "write headers")
	}

	i := 0
	// chunks of 8
	buf := make([]byte, 64)
	for i < len(targets) {
		if i != 0 && i%8 == 0 {
			if _, err := l.bufw.Write(buf); err != nil {
				return errors.Wrap(err, "write link chunk")
			}
		}

		pos := i % 8
		start := pos * 8
		end := start + 8
		binary.LittleEndian.PutUint64(buf[start:end], targets[i])

		i++
	}

	// remainder
	if i%8 != 0 {
		start := 0
		end := i % 8 * 8

		if _, err := l.bufw.Write(buf[start:end]); err != nil {
			return errors.Wrap(err, "write link remainder")
		}
	}

	return nil
}

func (l *Logger) AddTombstone(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(AddTombstone)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite)
	return err
}

func (l *Logger) RemoveTombstone(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(RemoveTombstone)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite)
	return err
}

func (l *Logger) ClearLinks(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(ClearLinks)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite)
	return err
}

func (l *Logger) DeleteNode(id uint64) error {
	toWrite := make([]byte, 9)
	toWrite[0] = byte(DeleteNode)
	binary.LittleEndian.PutUint64(toWrite[1:9], id)
	_, err := l.bufw.Write(toWrite)
	return err
}

func (l *Logger) Reset() error {
	toWrite := make([]byte, 1)
	toWrite[0] = byte(ResetIndex)
	_, err := l.bufw.Write(toWrite)
	return err
}
