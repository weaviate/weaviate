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
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/gomlx/go-huggingface/hub"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

const defaultBatchSize = 1000

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

// TODO: Clean up this abstraction..
type ParquetFile struct {
	localPath     string
	file          *os.File
	parquetFile   *parquet.File
	columnIndices map[string]int
	numRows       int
}

func (p *ParquetFile) Close() {
	p.file.Close()
}

func (h *HubDataset) downloadAndOpenParquetFile(name string) (*ParquetFile, error) {
	localFile, err := h.downloadParquetFile(name)
	if err != nil {
		return nil, fmt.Errorf("failed to download file %s: %w", localFile, err)
	}
	// Open the parquet file
	file, err := os.Open(localFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", localFile, err)
	}
	shouldClose := true
	defer func() {
		if shouldClose {
			file.Close()
		}
	}()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %w", localFile, err)
	}
	fileSize := fileInfo.Size()

	// Open as parquet file with optimizations
	pqFile, err := parquet.OpenFile(file, fileSize,
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file %s: %w", localFile, err)
	}
	shouldClose = false
	columnIndices, numRows := h.parquetMetadata(pqFile)
	return &ParquetFile{
		localPath:     localFile,
		file:          file,
		parquetFile:   pqFile,
		columnIndices: columnIndices,
		numRows:       numRows,
	}, nil
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

func (h *HubDataset) LoadTrainData() (ids []uint64, vectors [][]float32, err error) {
	file, err := h.downloadAndOpenParquetFile("train")
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// Validate required columns exist
	if err := h.validateColumns(file.columnIndices, "id", "embedding"); err != nil {
		return nil, nil, err
	}

	embeddingColIdx := file.columnIndices["embedding"]
	idColIdx := file.columnIndices["id"]

	ids = make([]uint64, 0, file.numRows)
	vectorBuilder := NewVectorBuilderFloat32(file.numRows)

	const defaultValueBufferSize = 1024
	buffer := make([]parquet.Value, defaultValueBufferSize)
	for _, rowGroup := range file.parquetFile.RowGroups() {
		for _, columnChunk := range rowGroup.ColumnChunks() {
			colIdx := columnChunk.Column()
			if colIdx != idColIdx && colIdx != embeddingColIdx {
				continue
			}
			valueReader := parquet.NewColumnChunkValueReader(columnChunk)
			defer valueReader.Close()
			for {
				n, err := valueReader.ReadValues(buffer)
				if err != nil && !errors.Is(err, io.EOF) {
					return nil, nil, fmt.Errorf("failed to read rows: %w", err)
				}
				if n > 0 {
					switch colIdx {
					case idColIdx:
						for i := range n {
							ids = append(ids, uint64(buffer[i].Int64()))
						}
					case embeddingColIdx:
						for i := range n {
							vectorBuilder.Add(buffer[i])
						}
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
			}
		}
	}

	// Returns an error if we did not produce numRows vectors of the same length
	vectors, err = vectorBuilder.AllVectors()
	if err != nil {
		return nil, nil, err
	}
	if len(ids) != file.numRows {
		return nil, nil, fmt.Errorf("read %d ids from parquet file, expected %d", len(ids), file.numRows)
	}
	return ids, vectors, nil
}

func (h *HubDataset) LoadTestData() (neighbors [][]uint64, vectors [][]float32, err error) {
	file, err := h.downloadAndOpenParquetFile("test")
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// Validate required columns exist
	if err := h.validateColumns(file.columnIndices, "embedding", "neighbors"); err != nil {
		return nil, nil, err
	}

	// Create reader for the parquet file
	reader := parquet.NewReader(file.parquetFile)
	defer reader.Close()

	vectors = make([][]float32, 0, file.numRows)
	neighbors = make([][]uint64, 0, file.numRows)

	embeddingColIndex := file.columnIndices["embedding"]
	neighborsColIndex := file.columnIndices["neighbors"]

	rows := make([]parquet.Row, defaultBatchSize)
	for {
		n, err := reader.ReadRows(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("failed to read rows: %w", err)
		}
		for i := 0; i < n; i++ {
			row := rows[i]
			row.Range(func(columnIndex int, columnValues []parquet.Value) bool {
				switch columnIndex {
				case embeddingColIndex:
					// Extract embedding
					vector := make([]float32, len(columnValues))
					for j, value := range columnValues {
						vector[j] = value.Float()
					}
					vectors = append(vectors, vector)
				case neighborsColIndex:
					// Extract all neighbor values for this row
					var currentNeighbors []uint64
					for _, value := range columnValues {
						currentNeighbors = append(currentNeighbors, uint64(value.Int64()))
					}
					neighbors = append(neighbors, currentNeighbors)
				}
				return true // Continue processing
			})
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}

	if len(vectors) == 0 {
		return nil, nil, fmt.Errorf("no vectors loaded - check parquet schema and data")
	}

	return neighbors, vectors, nil
}
