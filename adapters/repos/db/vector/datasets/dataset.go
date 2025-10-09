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

package datasets

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/gomlx/go-huggingface/hub"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const defaultRowBufferSize = 1000

type HubDataset struct {
	repo      *hub.Repo
	datasetID string
	subset    string
	logger    logrus.FieldLogger
}

func NewHubDataset(datasetID string, subset string, logger logrus.FieldLogger) *HubDataset {
	hfAuthToken := os.Getenv("HF_TOKEN")
	if hfAuthToken == "" {
		logger.Warn("HF_TOKEN environment variable not set. Some datasets may require authentication.")
	}
	repo := hub.New(datasetID).
		WithType(hub.RepoTypeDataset).
		WithAuth(hfAuthToken)
	return &HubDataset{
		repo:      repo,
		datasetID: datasetID,
		subset:    subset,
		logger:    logger,
	}
}

func (h *HubDataset) parquetMetadata(pqFile *parquet.File) (map[string]int, int) {
	schema := pqFile.Schema()
	columnIndices := make(map[string]int)
	for i, col := range schema.Columns() {
		colName := col[0] // Top-level column name
		columnIndices[colName] = i
	}

	h.logger.Debugf("Schema: %+v", schema)
	h.logger.Debugf("Column Indices: %+v", columnIndices)

	numRows := int(pqFile.NumRows())
	h.logger.Infof("Number of rows: %d", numRows)
	return columnIndices, numRows
}

func (h *HubDataset) downloadParquetFile(name string) (string, error) {
	filePath := fmt.Sprintf("%s/%s/%s.parquet", h.subset, name, name)

	h.logger.Infof("Starting download of %s", filePath)
	startTime := time.Now()

	localPath, err := h.repo.DownloadFile(filePath)

	duration := time.Since(startTime)
	if err != nil {
		h.logger.Errorf("Failed to download %s after %v: %v", filePath, duration, err)
		return "", err
	}

	h.logger.Infof("Successfully downloaded %s in %v", filePath, duration)
	return localPath, nil
}

func (h *HubDataset) validateColumns(columnIndices map[string]int, requiredColumns ...string) error {
	for _, col := range requiredColumns {
		if _, exists := columnIndices[col]; !exists {
			return fmt.Errorf("required column '%s' not found", col)
		}
	}
	return nil
}

// Row reader that is used internally when reading both training and test data.
type parquetRowReader struct {
	osFile        *os.File
	pqReader      *parquet.Reader //nolint:staticcheck
	columnIndices map[string]int
	numRows       int
	numRowsRead   int
}

// Downloads the specified file and opens it for reading. If no errors were
// encountered it returns a parquetRowReader must be closed (by calling close()
// on the reader) after use.
func (h *HubDataset) newParquetRowReader(name string) (*parquetRowReader, error) {
	localFile, err := h.downloadParquetFile(name)
	if err != nil {
		return nil, fmt.Errorf("failed to download file %s: %w", localFile, err)
	}
	// Open the parquet file
	osFile, err := os.Open(localFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", localFile, err)
	}
	shouldClose := true
	defer func() {
		if shouldClose {
			osFile.Close()
		}
	}()

	// Get the file size in order to open the file as a parquet file (parquet
	// file metadata is placed at the back of the file)
	fileInfo, err := osFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %w", localFile, err)
	}
	fileSize := fileInfo.Size()

	// Open as parquet file with optimizations
	pqFile, err := parquet.OpenFile(osFile, fileSize,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file %s: %w", localFile, err)
	}

	columnIndices, numRows := h.parquetMetadata(pqFile)
	pqReader := parquet.NewReader(pqFile)
	shouldClose = false
	return &parquetRowReader{
		osFile:        osFile,
		pqReader:      pqReader,
		columnIndices: columnIndices,
		numRows:       numRows,
	}, nil
}

func (r *parquetRowReader) readRows(buffer []parquet.Row) (int, error) {
	n, err := r.pqReader.ReadRows(buffer)
	r.numRowsRead += n
	return n, err
}

func (r *parquetRowReader) close() {
	r.pqReader.Close()
	r.osFile.Close()
}

type DataReader struct {
	split           Split
	rowReader       *parquetRowReader
	rowBuffer       []parquet.Row
	idColIdx        int
	embeddingColIdx int
	neighborColIdx  int
}

type Split string

const (
	TrainSplit Split = "train"
	TestSplit  Split = "test"
)

func (h *HubDataset) NewDataReader(split Split, bufferSize int) (*DataReader, error) {
	rowReader, err := h.newParquetRowReader(string(split))
	if err != nil {
		return nil, err
	}
	closeRowReader := true
	defer func() {
		if closeRowReader {
			rowReader.close()
		}
	}()

	// Validate whether the expected columns are present in the parquet schema.
	if err := h.validateColumns(rowReader.columnIndices, "id", "embedding"); err != nil {
		return nil, err
	}
	idColIdx, embeddingColIdx := rowReader.columnIndices["id"], rowReader.columnIndices["embedding"]

	neighborColIdx := -1
	if split == TestSplit {
		if err := h.validateColumns(rowReader.columnIndices, "neighbors"); err != nil {
			return nil, err
		}
		neighborColIdx = rowReader.columnIndices["neighbors"]
	}

	closeRowReader = false
	return &DataReader{
		split:           split,
		rowReader:       rowReader,
		rowBuffer:       make([]parquet.Row, bufferSize),
		idColIdx:        idColIdx,
		embeddingColIdx: embeddingColIdx,
		neighborColIdx:  neighborColIdx,
	}, nil
}

func (r *DataReader) Close() {
	r.rowReader.close()
}

func (r *DataReader) NumRows() int {
	return r.rowReader.numRows
}

func convertBinaryToFloat32(data []byte) ([]float32, error) {
	if len(data)%4 != 0 {
		return nil, fmt.Errorf("binary data length %d is not divisible by 4 (float32 size)", len(data))
	}
	result := make([]float32, len(data)/4)
	for i := 0; i < len(result); i++ {
		offset := i * 4
		bits := binary.LittleEndian.Uint32(data[offset : offset+4])
		result[i] = math.Float32frombits(bits)
	}
	return result, nil
}

// A chunk of data read from a parquet file.
type DataChunk struct {
	Split     Split
	IDs       []uint64
	Vectors   [][]float32
	Neighbors [][]uint64
	RowOffset int // Index of the first row in this chunk (into the list of rows produced by the parquet file)
	NumRows   int // Number of rows in this chunk
}

// Attempt to read bufferSize rows from the underlying parquet file and return a
// chunk of heap-allocated training data.
//
// If any other error than io.EOF is returned then no data is returned and the
// reader should be considered corrupt and abandoned. The io.EOF error may be
// returned with a non-empty batch of data for the last batch.
func (r *DataReader) ReadNextChunk() (*DataChunk, error) {
	// Note: It is valid for the reader to return both a non-zero number of rows
	// and a non-nil error (including io.EOF).
	n, err := r.rowReader.readRows(r.rowBuffer)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	ids := make([]uint64, 0, n)
	vectors := make([][]float32, 0, n)
	var neighbors [][]uint64
	if r.split == TestSplit {
		neighbors = make([][]uint64, 0, n)
	}

	for i := range n {
		var rowErr error // err might hold io.EOF that we want to preserve
		row := r.rowBuffer[i]
		row.Range(func(colIdx int, colValues []parquet.Value) bool {
			switch colIdx {
			case r.idColIdx:
				if len(colValues) != 1 || colValues[0].Kind() != parquet.Int64 {
					rowErr = errors.New("wrong format or missing value in id column")
					return false
				}
				ids = append(ids, uint64(colValues[0].Int64()))
			case r.embeddingColIdx:
				if len(colValues) != 1 || colValues[0].Kind() != parquet.ByteArray {
					rowErr = errors.New("wrong format or missing value in embedding column")
					return false
				}
				binaryData := colValues[0].ByteArray()
				convertedVector, rowErr := convertBinaryToFloat32(binaryData)
				if rowErr != nil {
					return false // Stop processing on error
				}
				vectors = append(vectors, convertedVector)
			case r.neighborColIdx:
				const expectedNeighbors = 100
				if len(colValues) != expectedNeighbors || colValues[0].Kind() != parquet.Int64 {
					rowErr = fmt.Errorf("failed to find a list of %d int64 values in the neighbor column", expectedNeighbors)
				}
				rowNeighbors := make([]uint64, expectedNeighbors)
				for j, v := range colValues {
					rowNeighbors[j] = uint64(v.Int64())
				}
				neighbors = append(neighbors, rowNeighbors)
			}
			return true // Continue processing
		})
		if rowErr != nil {
			return nil, rowErr
		}
	}

	// Basic validation of the data
	if len(ids) != n {
		return nil, fmt.Errorf("chunk contains %d ids, expected %d", len(ids), n)
	}

	if len(vectors) != n {
		return nil, fmt.Errorf("chunk contains %d vectors, expected %d", len(ids), n)
	}

	if r.split == TestSplit && len(neighbors) != n {
		return nil, fmt.Errorf("chunk contains %d lists of neighbors, expected %d", len(ids), n)
	}

	if n > 0 {
		d := len(vectors[0])
		for _, v := range vectors {
			if len(v) != d {
				return nil, fmt.Errorf("vectors of different lengths: %d and %d", d, len(v))
			}
		}
	}

	chunk := &DataChunk{
		Split:     r.split,
		IDs:       ids,
		Vectors:   vectors,
		Neighbors: neighbors,
		RowOffset: r.rowReader.numRowsRead - n,
		NumRows:   n,
	}
	return chunk, err
}

func (h *HubDataset) LoadTrainData() (ids []uint64, vectors [][]float32, err error) {
	trainReader, err := h.NewDataReader(TrainSplit, defaultRowBufferSize)
	if err != nil {
		return nil, nil, err
	}
	defer trainReader.Close()
	n := trainReader.NumRows()
	ids = make([]uint64, 0, n)
	vectors = make([][]float32, 0, n)
	for {
		chunk, err := trainReader.ReadNextChunk()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		ids = append(ids, chunk.IDs...)
		vectors = append(vectors, chunk.Vectors...)
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return ids, vectors, nil
}

func (h *HubDataset) LoadTestData() (neighbors [][]uint64, vectors [][]float32, err error) {
	trainReader, err := h.NewDataReader(TestSplit, defaultRowBufferSize)
	if err != nil {
		return nil, nil, err
	}
	defer trainReader.Close()
	n := trainReader.NumRows()
	neighbors = make([][]uint64, 0, n)
	vectors = make([][]float32, 0, n)
	for {
		chunk, err := trainReader.ReadNextChunk()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, err
		}
		neighbors = append(neighbors, chunk.Neighbors...)
		vectors = append(vectors, chunk.Vectors...)
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return neighbors, vectors, nil
}
