package segmentindex

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/usecases/integrity"
)

type SegmentFile struct {
	header         *Header
	writer         *bufio.Writer
	checksumWriter integrity.ChecksumWriter
}

func NewSegmentFile(bufw *bufio.Writer) *SegmentFile {
	return &SegmentFile{
		writer:         bufw,
		checksumWriter: integrity.NewCRC32Writer(bufw),
	}
}

func (f *SegmentFile) ChecksumWriter() io.Writer {
	return f.checksumWriter
}

func (f *SegmentFile) Flush() error {
	if err := f.writer.Flush(); err != nil {
		return fmt.Errorf("flush segmentfile: %w", err)
	}
	return nil
}

func (f *SegmentFile) WriteHeader(header *Header) (int64, error) {
	// We save the header, and only write it to the checksum at the end
	f.header = header
	n, err := header.WriteTo(f.writer)
	if err != nil {
		return n, fmt.Errorf("write segment file header: %w", err)
	}
	return n, nil
}

func (f *SegmentFile) WriteIndexes(indexes *Indexes) (int64, error) {
	n, err := indexes.WriteTo(f.checksumWriter)
	if err != nil {
		return n, fmt.Errorf("write segment file indexes: %w", err)
	}
	return n, nil
}

func (f *SegmentFile) WriteChecksum() (int64, error) {
	b := bytes.NewBuffer(make([]byte, HeaderSize))
	if _, err := f.header.WriteTo(b); err != nil {
		return 0, fmt.Errorf(
			"serialize segment header to write to checksum hash: %w", err)
	}
	if _, err := f.checksumWriter.HashWrite(b.Bytes()); err != nil {
		return 0, fmt.Errorf("write segment header to checksum hash: %w", err)
	}
	n, err := f.writer.Write(f.checksumWriter.Hash())
	if err != nil {
		return int64(n), fmt.Errorf("write segment file checksum: %w", err)
	}
	return int64(n), nil
}
