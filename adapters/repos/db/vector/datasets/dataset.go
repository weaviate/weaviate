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
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/gomlx/go-huggingface/hub"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const defaultBatchSize = 1000

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

func (h *HubDataset) validateColumns(columnIndices map[string]int, requiredColumns ...string) error {
	for _, col := range requiredColumns {
		if _, exists := columnIndices[col]; !exists {
			return fmt.Errorf("required column '%s' not found", col)
		}
	}
	return nil
}

func (h *HubDataset) LoadTrainingData() (ids []uint64, vectors [][]float32, err error) {
	localFile, err := h.downloadParquetFile("train")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download file %s: %w", localFile, err)
	}
	// Open the parquet file
	file, err := os.Open(localFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %w", localFile, err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get file info for %s: %w", localFile, err)
	}
	fileSize := fileInfo.Size()

	// Open as parquet file with optimizations
	pqFile, err := parquet.OpenFile(file, fileSize,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open parquet file %s: %w", localFile, err)
	}

	columnIndices, numRows := h.parquetMetadata(pqFile)

	// Validate required columns exist
	if err := h.validateColumns(columnIndices, "embedding", "id"); err != nil {
		return nil, nil, err
	}

	// Create reader for the parquet file
	reader := parquet.NewReader(pqFile)
	defer reader.Close()

	vectors = make([][]float32, 0, numRows)
	ids = make([]uint64, 0, numRows)

	embeddingColIndex := columnIndices["embedding"]
	idColIndex := columnIndices["id"]

	batchSize := defaultBatchSize
	totalRead := 0

	for totalRead < numRows {
		remaining := numRows - totalRead
		currentBatchSize := batchSize
		if remaining < batchSize {
			currentBatchSize = remaining
		}

		rows := make([]parquet.Row, currentBatchSize)
		n, err := reader.ReadRows(rows)
		if err != nil && err != io.EOF {
			return nil, nil, fmt.Errorf("failed to read rows: %w", err)
		}

		if n == 0 {
			break
		}

		// Process batch using Row.Range for proper column handling
		for i := 0; i < n; i++ {
			row := rows[i]

			// Use Row.Range to properly handle columns
			row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
				switch {
				case columnIndex == idColIndex:
					// Extract ID
					if len(columnValues) > 0 && columnValues[0].Kind() == parquet.Int64 {
						ids = append(ids, uint64(columnValues[0].Int64()))
					}
				case columnIndex == embeddingColIndex:
					// Extract embedding
					if len(columnValues) > 0 {
						binaryData := columnValues[0].ByteArray()
						if len(binaryData) > 0 {
							convertedVector, err := convertBinaryToFloat32(binaryData)
							if err != nil {
								return false // Stop processing on error
							}
							vectors = append(vectors, convertedVector)
						}
					}
				}
				return true // Continue processing
			})
		}

		totalRead += n

		if totalRead%10000 == 0 {
			h.logger.Debugf("Processed %d rows", totalRead)
		}

		// Break if we got EOF or read fewer rows than expected
		if err != nil && err == io.EOF {
			break
		}
	}

	h.logger.Infof("Loaded %d vectors", len(vectors))

	if len(vectors) == 0 {
		return nil, nil, fmt.Errorf("no vectors loaded - check parquet schema and data")
	}

	return ids, vectors, nil
}

func (h *HubDataset) LoadTestData() (neighbors [][]uint64, vectors [][]float32, err error) {
	localFile, err := h.downloadParquetFile("test")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download file %s: %w", localFile, err)
	}
	// Open the parquet file
	file, err := os.Open(localFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %w", localFile, err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get file info for %s: %w", localFile, err)
	}
	fileSize := fileInfo.Size()

	// Open as parquet file with optimizations
	pqFile, err := parquet.OpenFile(file, fileSize,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open parquet file %s: %w", localFile, err)
	}

	columnIndices, numRows := h.parquetMetadata(pqFile)

	// Validate required columns exist
	if err := h.validateColumns(columnIndices, "embedding", "neighbors"); err != nil {
		return nil, nil, err
	}

	// Create reader for the parquet file
	reader := parquet.NewReader(pqFile)
	defer reader.Close()

	vectors = make([][]float32, 0, numRows)
	neighbors = make([][]uint64, 0, numRows)

	embeddingColIndex := columnIndices["embedding"]
	neighborsColIndex := columnIndices["neighbors"]

	batchSize := defaultBatchSize
	totalRead := 0

	for totalRead < numRows {
		remaining := numRows - totalRead
		currentBatchSize := batchSize
		if remaining < batchSize {
			currentBatchSize = remaining
		}

		rows := make([]parquet.Row, currentBatchSize)
		n, err := reader.ReadRows(rows)
		if err != nil && err != io.EOF {
			return nil, nil, fmt.Errorf("failed to read rows: %w", err)
		}

		if n == 0 {
			break
		}

		// Process batch using Row.Range for proper column handling
		for i := 0; i < n; i++ {
			row := rows[i]
			var currentNeighbors []uint64

			// Use Row.Range to properly handle columns
			row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
				switch {
				case columnIndex == embeddingColIndex:
					// Extract embedding
					if len(columnValues) > 0 {
						binaryData := columnValues[0].ByteArray()
						if len(binaryData) > 0 {
							convertedVector, err := convertBinaryToFloat32(binaryData)
							if err != nil {
								return false // Stop processing on error
							}
							vectors = append(vectors, convertedVector)
						}
					}
				case columnIndex == neighborsColIndex:
					// Extract all neighbor values for this row
					for _, value := range columnValues {
						if value.Kind() == parquet.Int64 {
							currentNeighbors = append(currentNeighbors, uint64(value.Int64()))
						}
					}
				}
				return true // Continue processing
			})

			// Add the neighbors for this row
			neighbors = append(neighbors, currentNeighbors)
		}

		totalRead += n

		if totalRead%10000 == 0 {
			h.logger.Debugf("Processed %d rows", totalRead)
		}

		// Break if we got EOF or read fewer rows than expected
		if err != nil && err == io.EOF {
			break
		}
	}

	h.logger.Infof("Loaded %d vectors", len(vectors))

	if len(vectors) == 0 {
		return nil, nil, fmt.Errorf("no vectors loaded - check parquet schema and data")
	}

	return neighbors, vectors, nil
}
