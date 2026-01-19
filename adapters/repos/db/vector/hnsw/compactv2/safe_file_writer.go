//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compactv2

import (
	"bufio"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/diskio"
)

const (
	// DefaultBufferSize is the default buffer size for SafeFileWriter (1MB).
	DefaultBufferSize = 1024 * 1024

	// tempFileSuffix is the suffix used for temporary files during atomic writes.
	tempFileSuffix = ".tmp"
)

// SafeFileWriter handles crash-safe atomic file creation.
// Files are written to a .tmp path, then atomically renamed on commit.
//
// Usage:
//
//	sfw, err := compactv2.NewSafeFileWriter(snapshotPath, compactv2.DefaultBufferSize)
//	if err != nil {
//	    return err
//	}
//	defer sfw.Abort() // cleanup on error path
//
//	writer := compactv2.NewSnapshotWriter(sfw.Writer())
//	if err := writer.WriteFromMerger(merger); err != nil {
//	    return err
//	}
//
//	return sfw.Commit()
type SafeFileWriter struct {
	tmpPath   string
	finalPath string
	file      *os.File
	buffered  *bufio.Writer
	committed bool
	closed    bool
}

// NewSafeFileWriter creates a new crash-safe file writer.
// The file is created at finalPath + ".tmp" with O_CREATE|O_EXCL|O_WRONLY.
// O_EXCL ensures we fail if temp file exists (detects incomplete previous writes).
func NewSafeFileWriter(finalPath string, bufferSize int) (*SafeFileWriter, error) {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	tmpPath := finalPath + tempFileSuffix

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o666)
	if err != nil {
		return nil, errors.Wrapf(err, "create temp file %s", tmpPath)
	}

	return &SafeFileWriter{
		tmpPath:   tmpPath,
		finalPath: finalPath,
		file:      file,
		buffered:  bufio.NewWriterSize(file, bufferSize),
	}, nil
}

// Writer returns a buffered writer for the underlying file.
func (s *SafeFileWriter) Writer() io.Writer {
	return s.buffered
}

// Commit finalizes the file: flushes buffer, fsyncs file, renames to final path,
// and fsyncs the parent directory.
//
// After Commit returns successfully, the file is guaranteed to be durable
// and visible at the final path.
func (s *SafeFileWriter) Commit() error {
	if s.committed {
		return nil
	}
	if s.closed {
		return errors.New("cannot commit: file already closed")
	}

	// 1. Flush buffered writer
	if err := s.buffered.Flush(); err != nil {
		return errors.Wrap(err, "flush buffer")
	}

	// 2. Sync file to disk
	if err := s.file.Sync(); err != nil {
		return errors.Wrap(err, "sync file")
	}

	// 3. Close file (required before rename on some systems)
	if err := s.file.Close(); err != nil {
		return errors.Wrap(err, "close file")
	}
	s.closed = true

	// 4. Atomic rename
	if err := os.Rename(s.tmpPath, s.finalPath); err != nil {
		return errors.Wrapf(err, "rename %s to %s", s.tmpPath, s.finalPath)
	}

	// 5. Sync parent directory
	if err := diskio.Fsync(filepath.Dir(s.finalPath)); err != nil {
		return errors.Wrap(err, "sync directory")
	}

	s.committed = true
	return nil
}

// Abort cleans up the temporary file without committing.
// Safe to call multiple times or after Commit.
func (s *SafeFileWriter) Abort() error {
	if s.committed {
		return nil
	}

	// Close file if not already closed (ignore errors)
	if !s.closed {
		s.file.Close()
		s.closed = true
	}

	// Remove temp file (ignore errors - may not exist)
	os.Remove(s.tmpPath)

	return nil
}

// CleanupOrphanedTempFiles removes any orphaned .tmp files in the given directory.
// This should be called during index startup to clean up from previous incomplete writes.
func CleanupOrphanedTempFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "read directory %s", dir)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if len(name) > len(tempFileSuffix) && name[len(name)-len(tempFileSuffix):] == tempFileSuffix {
			tmpPath := filepath.Join(dir, name)
			if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
				return errors.Wrapf(err, "remove orphaned temp file %s", tmpPath)
			}
		}
	}

	return nil
}
