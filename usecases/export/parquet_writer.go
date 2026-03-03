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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ParquetRow represents a single row in the Parquet file
type ParquetRow struct {
	ID           string `parquet:"id,dict"`
	ClassName    string `parquet:"class_name,dict"`
	CreationTime int64  `parquet:"creation_time"`
	UpdateTime   int64  `parquet:"update_time"`
	Vector       []byte `parquet:"vector,optional"`
	NamedVectors []byte `parquet:"named_vectors,optional"`
	MultiVectors []byte `parquet:"multi_vectors,optional"`
	Properties   []byte `parquet:"properties,optional"`
}

const (
	defaultBatchSize = 10000 // Buffer 10k rows before writing
)

// ParquetWriter writes Weaviate objects to Parquet format
type ParquetWriter struct {
	writer    *parquet.GenericWriter[ParquetRow]
	buffer    []ParquetRow
	batchSize int
	written   int64
}

// NewParquetWriter creates a new Parquet writer
func NewParquetWriter(w io.Writer) (*ParquetWriter, error) {
	schema := parquet.SchemaOf(ParquetRow{})

	writer := parquet.NewGenericWriter[ParquetRow](w,
		schema,
		parquet.Compression(&parquet.Zstd),
		parquet.PageBufferSize(8*1024*1024), // 8MB page buffer
	)

	return &ParquetWriter{
		writer:    writer,
		buffer:    make([]ParquetRow, 0, defaultBatchSize),
		batchSize: defaultBatchSize,
	}, nil
}

// WriteObject writes a single object to the Parquet file (buffered)
func (pw *ParquetWriter) WriteObject(obj *storobj.Object) error {
	row, err := convertToParquetRow(obj)
	if err != nil {
		return fmt.Errorf("convert object to parquet row: %w", err)
	}

	pw.buffer = append(pw.buffer, row)

	// Flush if buffer is full
	if len(pw.buffer) >= pw.batchSize {
		return pw.Flush()
	}

	return nil
}

// Flush writes all buffered rows to the Parquet file
func (pw *ParquetWriter) Flush() error {
	if len(pw.buffer) == 0 {
		return nil
	}

	n, err := pw.writer.Write(pw.buffer)
	if err != nil {
		return fmt.Errorf("write batch to parquet: %w", err)
	}

	pw.written += int64(n)
	pw.buffer = pw.buffer[:0] // Reset buffer
	return nil
}

// Close flushes remaining data and closes the writer
func (pw *ParquetWriter) Close() error {
	if err := pw.Flush(); err != nil {
		return err
	}
	return pw.writer.Close()
}

// ObjectsWritten returns the total number of objects written
func (pw *ParquetWriter) ObjectsWritten() int64 {
	return pw.written
}

// convertToParquetRow converts a storobj.Object to a ParquetRow
func convertToParquetRow(obj *storobj.Object) (ParquetRow, error) {
	row := ParquetRow{
		ID:           obj.ID().String(),
		ClassName:    obj.Class().String(),
		CreationTime: obj.CreationTimeUnix(),
		UpdateTime:   obj.LastUpdateTimeUnix(),
	}

	// Serialize primary vector
	if len(obj.Vector) > 0 {
		row.Vector = serializeVector(obj.Vector)
	}

	// Serialize named vectors
	if len(obj.Vectors) > 0 {
		data, err := json.Marshal(obj.Vectors)
		if err != nil {
			return row, fmt.Errorf("marshal named vectors: %w", err)
		}
		row.NamedVectors = data
	}

	// Serialize multi vectors
	if len(obj.MultiVectors) > 0 {
		data, err := json.Marshal(obj.MultiVectors)
		if err != nil {
			return row, fmt.Errorf("marshal multi vectors: %w", err)
		}
		row.MultiVectors = data
	}

	// Serialize properties
	if obj.Properties() != nil {
		data, err := json.Marshal(obj.Properties())
		if err != nil {
			return row, fmt.Errorf("marshal properties: %w", err)
		}
		row.Properties = data
	}

	return row, nil
}

// serializeVector converts a []float32 vector to binary format
func serializeVector(vector []float32) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, vector)
	return buf.Bytes()
}
