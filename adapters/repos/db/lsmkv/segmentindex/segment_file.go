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

package segmentindex

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/usecases/integrity"
)

type SegmentWriter interface {
	Write(p []byte) (n int, err error)
	Flush() error
}

// SegmentFile facilitates the writing/reading of an LSM bucket segment file.
//
// These contents include the CRC32 checksum which is calculated based on the:
//   - segment data
//   - segment indexes
//   - segment header
//
// The checksum is calculated using those components in that exact ordering.
// This is because during compactions, the header is not actually known until
// the compaction process is complete. So to accommodate this, all segment
// checksum calculations are made using the header last.
//
// Usage:
//
//	To write a segment file, initialization and API are as follows:
//	   ```
//	   sf := NewSegmentFile(WithBufferedWriter(<some buffered writer>))
//	   sf.WriterHeader(<some *Header>)
//	   <some segment node>.WriteTo(sf.BodyWriter())
//	   sf.WriteChecksum()
//	   ```
//
//	To validate a segment file checksum, initialization and API are as follows:
//	   ```
//	   sf := NewSegmentFile(WithReader(<segment fd>))
//	   sf.ValidateChecksum(<segment fd file info>)
//	   ```
type SegmentFile struct {
	header         *Header
	writer         SegmentWriter
	reader         *bufio.Reader
	checksumWriter integrity.ChecksumWriter
	checksumReader integrity.ChecksumReader
	// flag to indicate if the segment file is empty.
	// this is necessary, because in the case of
	// compactions, we don't want to re-write the header
	// when it is later re-written
	writtenTo         bool
	checksumsDisabled bool
}

type SegmentFileOption func(*SegmentFile)

// WithBufferedWriter sets the desired segment file writer
// This will typically wrap the segment *os.File
func WithBufferedWriter(writer SegmentWriter) SegmentFileOption {
	return func(segmentFile *SegmentFile) {
		segmentFile.writer = writer
		segmentFile.checksumWriter = integrity.NewCRC32Writer(writer)
	}
}

// WithReader sets the desired segment file reader.
// This will typically be the segment *os.File.
func WithReader(reader io.Reader) SegmentFileOption {
	return func(segmentFile *SegmentFile) {
		segmentFile.reader = bufio.NewReader(reader)
		segmentFile.checksumReader = integrity.NewCRC32Reader(reader)
	}
}

// WithChecksumsDisabled configures the segment file
// to be written without checksums
func WithChecksumsDisabled(disable bool) SegmentFileOption {
	return func(segmentFile *SegmentFile) {
		segmentFile.checksumsDisabled = disable
	}
}

// NewSegmentFile creates a new instance of SegmentFile.
// Be sure to include a writer or reader option depending on your needs.
func NewSegmentFile(opts ...SegmentFileOption) *SegmentFile {
	s := &SegmentFile{
		checksumsDisabled: true,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BodyWriter exposes the underlying writer which calculates the hash inline.
// This method is used when writing the body of the segment, the user data
// itself.
//
// Because there are many segment node types, and each exposes its own `WriteTo`
// (or similar) method, it would be cumbersome to support each node type, in the
// way we support WriteHeader and WriteIndexes. So this method exists to hook
// into each segment node's `WriteTo` instead.
//
// This method uses the written data to further calculate the checksum.
func (f *SegmentFile) BodyWriter() io.Writer {
	f.writtenTo = true

	if f.checksumsDisabled {
		return f.writer
	}
	return f.checksumWriter
}

// SetHeader sets the header in the SegmentFile without writing anything. This should be used if the header was already
// written by another reader.
func (f *SegmentFile) SetHeader(header *Header) {
	f.header = header
}

// WriteHeader writes the header struct to the underlying writer.
// This method resets the internal hash, so that the header can be written
// to the checksum last. For more details see SegmentFile.
func (f *SegmentFile) WriteHeader(header *Header) (int64, error) {
	if f.writer == nil {
		return 0, fmt.Errorf(" SegmentFile not initialized with a reader, " +
			"try adding one with segmentindex.WithBufferedWriter(*bufio.Writer)")
	}

	if f.checksumsDisabled {
		if f.writtenTo {
			return 0, nil
		}
		return header.WriteTo(f.writer)
	}

	f.header = header
	// If this is a memtable flush, we want to write the header up front.
	// If this is a compaction, the dummy header already exists, and will
	// be overwritten through a different writer. In that case, all we care
	// about is saving the header pointer, so we can add it to the hash when
	// WriteChecksum is called.
	if !f.writtenTo {
		n, err := header.WriteTo(f.checksumWriter)
		if err != nil {
			return n, fmt.Errorf("write segment file header: %w", err)
		}
		// We save the header, and only write it to the checksum at the end
		f.checksumWriter.Reset()
		f.writtenTo = true
		return n, nil
	}

	return 0, nil
}

// WriteIndexes writes the indexes struct to the underlying writer.
// This method uses the written data to further calculate the checksum.
func (f *SegmentFile) WriteIndexes(indexes *Indexes) (int64, error) {
	if f.writer == nil {
		return 0, fmt.Errorf(" SegmentFile not initialized with a reader, " +
			"try adding one with segmentindex.WithBufferedWriter(*bufio.Writer)")
	}

	if f.checksumsDisabled {
		return indexes.WriteTo(f.writer)
	}

	n, err := indexes.WriteTo(f.checksumWriter)
	if err != nil {
		return n, fmt.Errorf("write segment file indexes: %w", err)
	}
	f.writtenTo = true
	return n, nil
}

// WriteChecksum writes checksum itself to the segment file.
// As mentioned elsewhere in SegmentFile, the header is added to the checksum last.
// This method finally adds the header to the hash, and then writes the resulting
// checksum to the segment file.
func (f *SegmentFile) WriteChecksum() (int64, error) {
	if f.writer == nil {
		return 0, fmt.Errorf(" SegmentFile not initialized with a reader, " +
			"try adding one with segmentindex.WithBufferedWriter(*bufio.Writer)")
	}

	var n int
	var err error

	if !f.checksumsDisabled {
		if err = f.addHeaderToChecksum(); err != nil {
			return 0, err
		}

		n, err = f.writer.Write(f.checksumWriter.Hash())
		if err != nil {
			return int64(n), fmt.Errorf("write segment file checksum: %w", err)
		}
	}

	if err = f.writer.Flush(); err != nil {
		return 0, fmt.Errorf("flush segmentfile: %w", err)
	}

	return int64(n), nil
}

// ValidateChecksum determines if a segment's content matches its checksum
func (f *SegmentFile) ValidateChecksum(size int64) error {
	if f.reader == nil {
		return fmt.Errorf(" SegmentFile not initialized with a reader, " +
			"try adding one with segmentindex.WithReader(io.Reader)")
	}

	f.checksumReader = integrity.NewCRC32Reader(f.reader)

	var header [HeaderSize]byte
	_, err := f.reader.Read(header[:])
	if err != nil {
		return fmt.Errorf("read segment file header: %w", err)
	}

	var (
		buffer    = make([]byte, 4096) // Buffer for chunked reads
		dataSize  = size - HeaderSize - ChecksumSize
		remaining = dataSize
	)

	for remaining > 0 {
		toRead := int64(len(buffer))
		if remaining < toRead {
			toRead = remaining
		}

		n, err := f.checksumReader.Read(buffer[:toRead])
		if err != nil {
			return fmt.Errorf("read segment file: %w", err)
		}

		remaining -= int64(n)
	}

	var checksumBytes [ChecksumSize]byte
	_, err = f.reader.Read(checksumBytes[:])
	if err != nil {
		return fmt.Errorf("read segment file checksum: %w", err)
	}

	f.reader.Reset(bytes.NewReader(header[:]))
	_, err = f.checksumReader.Read(make([]byte, HeaderSize))
	if err != nil {
		return fmt.Errorf("add header to checksum: %w", err)
	}

	computedChecksum := f.checksumReader.Hash()
	if !bytes.Equal(computedChecksum, checksumBytes[:]) {
		return fmt.Errorf("invalid checksum")
	}

	return nil
}

func (f *SegmentFile) addHeaderToChecksum() error {
	b := bytes.NewBuffer(nil)
	if _, err := f.header.WriteTo(b); err != nil {
		return fmt.Errorf(
			"serialize segment header to write to checksum hash: %w", err)
	}
	if _, err := f.checksumWriter.HashWrite(b.Bytes()); err != nil {
		return fmt.Errorf("write segment header to checksum hash: %w", err)
	}
	return nil
}
