package queue

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// chunkSize is the maximum size of each chunk file.
// It is set to 144KB to be a multiple of 4KB (common page size)
// and of 9 bytes (size of a single record).
// This also works for larger page sizes (e.g. 16KB for macOS).
// The goal is to have a quick way of determining the number of records
// without having to read the entire file or to maintain an index.
const chunkSize = 144 * 1024

const partialChunkFile = "chunk.bin.partial"
const chunkFileFmt = "chunk-%d.bin"

type Encoder struct {
	logger logrus.FieldLogger
	dir    string

	m sync.Mutex
	w bufio.Writer
	f *os.File
}

func NewEncoder(dir string) (*Encoder, error) {
	e := Encoder{
		dir: dir,
	}

	// create the directory if it doesn't exist
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	return &e, nil
}

func (e *Encoder) Encode(op uint8, key uint64) (int, error) {
	e.m.Lock()
	defer e.m.Unlock()

	err := e.ensureChunk()
	if err != nil {
		return 0, err
	}

	err = binary.Write(&e.w, binary.LittleEndian, op)
	if err != nil {
		return 0, errors.Wrap(err, "failed to write op")
	}

	err = binary.Write(&e.w, binary.LittleEndian, key)
	if err != nil {
		return 0, errors.Wrap(err, "failed to write key")
	}

	return 0, nil
}

func (e *Encoder) ensureChunk() error {
	if e.f != nil && e.w.Size()+9 /* size of op + key */ > chunkSize {
		err := e.promoteChunkNoLock()
		if err != nil {
			return err
		}
	}

	if e.f == nil {
		// create or open partial chunk
		path := filepath.Join(e.dir, partialChunkFile)
		// open append mode, write only
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return errors.Wrap(err, "failed to create chunk file")
		}

		e.w.Reset(f)
		e.f = f
	}

	return nil
}

func (e *Encoder) promoteChunkNoLock() error {
	fName := e.f.Name()

	// flush and close current chunk
	err := e.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush chunk")
	}
	err = e.f.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close chunk")
	}

	e.f = nil

	// rename the file to remove the .partial suffix
	// and add a timestamp to the filename
	newPath := filepath.Join(e.dir, fmt.Sprintf(chunkFileFmt, time.Now().UnixNano()))
	err = os.Rename(fName, newPath)
	if err != nil {
		return errors.Wrap(err, "failed to rename chunk file")
	}

	e.logger.WithField("file", newPath).Debug("chunk file created")

	return nil
}

// This method is used by the scheduler to promote the current chunk
// when it's been stalled for too long.
func (e *Encoder) promoteChunk() error {
	e.m.Lock()
	defer e.m.Unlock()

	return e.promoteChunkNoLock()
}

func Decode(r *bufio.Reader) (uint8, uint64, error) {
	op, err := r.ReadByte()
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to read op")
	}

	var key uint64
	err = binary.Read(r, binary.LittleEndian, &key)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to read key")
	}

	return op, key, nil
}
