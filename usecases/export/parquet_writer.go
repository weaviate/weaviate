//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package export

import (
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// ParquetRow represents a single row in the Parquet file
type ParquetRow struct {
	ID           string `parquet:"id,dict"`
	CreationTime int64  `parquet:"creation_time"`
	UpdateTime   int64  `parquet:"update_time"`
	Vector       []byte `parquet:"vector,optional"`
	NamedVectors []byte `parquet:"named_vectors,optional"`
	MultiVectors []byte `parquet:"multi_vectors,optional"`
	Properties   []byte `parquet:"properties,optional"`
}

const (
	// WriteRow flushes its buffer at defaultBatchSize rows or maxBatchBytes.
	defaultBatchSize = 256
	maxBatchBytes    = 4 * 1024 * 1024

	// Flush ends the open row group at maxRowGroupBytes. parquet-go keeps that
	// row group in memory and writes it to the io.Writer only when it ends.
	maxRowGroupBytes = 32 * 1024 * 1024
)

// Blob columns NewParquetWriter writes without page statistics, so page headers
// don't copy each page's min and max blob. SkipPageBounds stays off because
// DataFusion skips matching pages when the column index bounds are empty.
var columnsWithoutPageStatistics = []string{"vector", "named_vectors", "multi_vectors", "properties"}

// ParquetWriter writes Weaviate objects to Parquet format
type ParquetWriter struct {
	writer        *parquet.GenericWriter[ParquetRow]
	buffer        []ParquetRow
	bufferBytes   int // ID and blob bytes of the rows in buffer
	batchSize     int
	rowGroupStart int64       // writer.Size() when the open row group started
	onFlush       func(int64) // called after each successful flush with the number of rows flushed
}

// NewParquetWriter creates a new Parquet writer
func NewParquetWriter(w io.Writer) (*ParquetWriter, error) {
	schema := parquet.SchemaOf(ParquetRow{})

	options := []parquet.WriterOption{
		schema,
		parquet.Compression(&parquet.Zstd),
		parquet.PageBufferSize(8 * 1024 * 1024), // 8MB page buffer
	}
	for _, column := range columnsWithoutPageStatistics {
		options = append(options, parquet.SkipPageStatistics(column))
	}
	writer := parquet.NewGenericWriter[ParquetRow](w, options...)

	return &ParquetWriter{
		writer:    writer,
		buffer:    make([]ParquetRow, 0, defaultBatchSize),
		batchSize: defaultBatchSize,
	}, nil
}

// WriteRow writes a pre-converted row to the Parquet file (buffered).
// This is used by the parallel export path where conversion happens in
// worker goroutines.
func (pw *ParquetWriter) WriteRow(row ParquetRow) error {
	pw.buffer = append(pw.buffer, row)
	pw.bufferBytes += len(row.ID) + len(row.Vector) + len(row.NamedVectors) + len(row.MultiVectors) + len(row.Properties)

	if len(pw.buffer) >= pw.batchSize || pw.bufferBytes >= maxBatchBytes {
		return pw.Flush()
	}

	return nil
}

// Flush writes all buffered rows to the open row group, and ends the row
// group once it reaches maxRowGroupBytes.
func (pw *ParquetWriter) Flush() error {
	if len(pw.buffer) == 0 {
		return nil
	}

	n := int64(len(pw.buffer))
	_, err := pw.writer.Write(pw.buffer)
	if err != nil {
		return fmt.Errorf("write batch to parquet: %w", err)
	}

	clear(pw.buffer) // drop the rows' byte slices so they can be garbage-collected
	pw.buffer = pw.buffer[:0]
	pw.bufferBytes = 0

	if pw.writer.Size()-pw.rowGroupStart >= maxRowGroupBytes {
		if err := pw.writer.Flush(); err != nil {
			return fmt.Errorf("write row group to parquet: %w", err)
		}
		pw.rowGroupStart = pw.writer.Size()
	}

	if pw.onFlush != nil {
		pw.onFlush(n)
	}
	return nil
}

// Close flushes remaining data and closes the writer
func (pw *ParquetWriter) Close() error {
	if err := pw.Flush(); err != nil {
		return err
	}
	return pw.writer.Close()
}

// Size estimates the file size from the bytes written and the open row group.
// It does not count rows still in the batch buffer.
func (pw *ParquetWriter) Size() int64 {
	return pw.writer.Size()
}

// SetFileMetadata sets a key/value pair in the Parquet file metadata.
func (pw *ParquetWriter) SetFileMetadata(key, value string) {
	pw.writer.SetKeyValueMetadata(key, value)
}
