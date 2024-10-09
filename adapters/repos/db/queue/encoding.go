package queue

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// chunkSize is the maximum size of each chunk file.
// It is set to 144KB to be a multiple of 4KB (common page size)
// and of 9 bytes (size of a single record).
// This also works for larger page sizes (e.g. 16KB for macOS).
const chunkSize = 144 * 1024

type Encoder struct {
	logger logrus.FieldLogger
	dir    string

	w bufio.Writer
	f *os.File
}

func NewEncoder(dir string) *Encoder {
	return &Encoder{
		dir: dir,
	}
}

func (e *Encoder) Encode(op uint8, key uint64) (int, error) {
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
		newPath := fName[:len(fName)-len(".partial")]
		err = os.Rename(fName, newPath)
		if err != nil {
			return errors.Wrap(err, "failed to rename chunk file")
		}

		e.logger.WithField("file", newPath).Debug("chunk file created")
	}

	if e.f == nil {
		// create new chunk
		path := filepath.Join(e.dir, fmt.Sprintf("chunk-%d.bin.partial", time.Now().UnixNano()))
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
