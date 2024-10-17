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

package queue

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// defaultChunkSize is the maximum size of each chunk file.
	// It is set to 144KB to be a multiple of 4KB (common page size)
	// and of 9 bytes (size of a single record).
	// This also works for larger page sizes (e.g. 16KB for macOS).
	// The goal is to have a quick way of determining the number of records
	// without having to read the entire file or to maintain an index.
	defaultChunkSize = 144 * 1024

	// name of the file that stores records before they reach the target size.
	partialChunkFile = "chunk.bin.partial"
	// chunkFileFmt is the format string for the promoted chunk files,
	// i.e. files that have reached the target size, or those that are
	// stale and need to be promoted.
	chunkFileFmt = "chunk-%d.bin"
)

// regex pattern for the chunk files
var chunkFilePattern = regexp.MustCompile(`chunk-\d+\.bin`)

type Encoder struct {
	logger    logrus.FieldLogger
	dir       string
	chunkSize int

	m                sync.RWMutex
	w                bufio.Writer
	f                *os.File
	partialChunkSize int
	recordCount      int64
}

func NewEncoder(dir string, logger logrus.FieldLogger) (*Encoder, error) {
	return NewEncoderWith(dir, logger, defaultChunkSize)
}

func NewEncoderWith(dir string, logger logrus.FieldLogger, chunkSize int) (*Encoder, error) {
	e := Encoder{
		dir:       dir,
		logger:    logger,
		chunkSize: chunkSize,
	}

	// create the directory if it doesn't exist
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	// determine the number of records stored on disk
	e.recordCount, err = e.calculateRecordCount()
	if err != nil {
		return nil, err
	}

	return &e, nil
}

func (e *Encoder) Encode(r *Record) error {
	e.m.Lock()
	defer e.m.Unlock()

	err := e.ensureChunk()
	if err != nil {
		return err
	}

	n, err := r.buf.WriteTo(&e.w)
	if err != nil {
		return errors.Wrap(err, "failed to write record")
	}

	e.partialChunkSize += int(n)
	e.recordCount++

	return nil
}

func (e *Encoder) Flush() error {
	e.m.Lock()
	defer e.m.Unlock()

	err := e.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush")
	}

	if e.f == nil {
		return nil
	}

	return e.f.Sync()
}

// Close the encoder and flush the current chunk.
func (e *Encoder) Close() error {
	err := e.Flush()
	if err != nil {
		return err
	}

	e.m.Lock()
	defer e.m.Unlock()

	if e.f != nil {
		err := e.f.Close()
		if err != nil {
			return errors.Wrap(err, "failed to close chunk")
		}

		e.f = nil
	}

	return nil
}

func (e *Encoder) Drop() error {
	e.m.Lock()
	defer e.m.Unlock()

	if e.f != nil {
		err := e.f.Close()
		if err != nil {
			e.logger.WithError(err).Error("failed to close chunk")
		}
	}

	// remove the directory
	err := os.RemoveAll(e.dir)
	if err != nil {
		return errors.Wrap(err, "failed to remove directory")
	}

	return nil
}

func (e *Encoder) ensureChunk() error {
	if e.f != nil && e.partialChunkSize+9 /* size of op + key */ > e.chunkSize {
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

		// get file size
		info, err := f.Stat()
		if err != nil {
			return errors.Wrap(err, "failed to stat chunk file")
		}

		e.partialChunkSize = int(info.Size())
	}

	return nil
}

func (e *Encoder) promoteChunkNoLock() error {
	if e.f == nil {
		return nil
	}

	fName := e.f.Name()

	// flush and close current chunk
	err := e.w.Flush()
	if err != nil {
		return errors.Wrap(err, "failed to flush chunk")
	}
	err = e.f.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync chunk")
	}
	err = e.f.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close chunk")
	}

	e.f = nil
	e.partialChunkSize = 0

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

// Returns the number of records stored on disk and in the partial chunk.
func (e *Encoder) RecordCount() int64 {
	e.m.RLock()
	defer e.m.RUnlock()

	return e.recordCount
}

// calculateRecordCount is a slow method that determines the number of records
// stored on disk and in the partial chunk, by reading the size of all the files in the directory.
// It is used when the encoder is first initialized.
func (e *Encoder) calculateRecordCount() (int64, error) {
	e.m.Lock()
	defer e.m.Unlock()

	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read directory")
	}

	var size int64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if entry.Name() != partialChunkFile && !chunkFilePattern.Match([]byte(entry.Name())) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return 0, errors.Wrap(err, "failed to get file info")
		}

		size += int64(info.Size()) / 9 /* size of a single record */
	}

	return size, nil
}

func (e *Encoder) removeChunk(path string) {
	e.m.Lock()
	defer e.m.Unlock()

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}

		e.logger.WithError(err).WithField("file", path).Warn("failed to stat chunk. trying to remove it anyway")
	}

	err = os.Remove(path)
	if err != nil {
		e.logger.WithError(err).WithField("file", path).Error("failed to remove chunk")
		return
	}

	e.recordCount -= int64(info.Size()) / 9

	e.logger.WithField("file", path).Debug("chunk removed")
}

func Decode(r *bufio.Reader) (uint8, uint64, error) {
	op, err := r.ReadByte()
	if err != nil {
		if err == io.EOF {
			return 0, 0, err
		}
		return 0, 0, errors.Wrap(err, "failed to read op")
	}

	var key uint64
	err = binary.Read(r, binary.LittleEndian, &key)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to read key")
	}

	return op, key, nil
}
