package datasets

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/gomlx/go-huggingface/hub"
	"github.com/parquet-go/parquet-go"
	"github.com/sirupsen/logrus"
)

var defaultBatchSize = 1000

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

func (h *HubDataset) LoadTrainingData() (ids []uint64, vectors [][]float32, err error) {
	// Download the dataset
	filePath := fmt.Sprintf("%s/train/train.parquet", h.subset)

	localFile, err := h.repo.DownloadFile(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to download file %s: %w", filePath, err)
	}
	h.logger.Debugf("Downloaded to: %s", localFile)

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

	// Get schema and column indices
	schema := pqFile.Schema()
	columnIndices := make(map[string]int)
	for i, col := range schema.Columns() {
		colName := col[0] // Top-level column name
		columnIndices[colName] = i
	}

	h.logger.Debugf("Schema: %+v", schema)
	h.logger.Debugf("Column Indices: %+v", columnIndices)

	// Read rows
	numRows := int(pqFile.NumRows())
	h.logger.Infof("Number of rows: %d", numRows)

	// Create reader for the parquet file
	reader := parquet.NewReader(pqFile)
	defer reader.Close()

	vectors = make([][]float32, 0, numRows)
	ids = make([]uint64, 0, numRows)

	embeddingColIndex := columnIndices["embedding"]
	idColIndex := columnIndices["id"]
	maxCol := max(embeddingColIndex, idColIndex)

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

		// Process batch with direct column access (optimized)
		for i := 0; i < n; i++ {
			row := rows[i]

			// Direct access to id and embedding columns
			if len(row) > maxCol {
				// Extract ID
				idValue := row[idColIndex]
				if idValue.Kind() == parquet.Int64 {
					ids = append(ids, uint64(idValue.Int64()))
				}

				// Extract embedding
				value := row[embeddingColIndex]
				binaryData := value.ByteArray()
				if len(binaryData) > 0 {
					convertedVector, err := convertBinaryToFloat32(binaryData)
					if err != nil {
						return nil, nil, fmt.Errorf("failed to convert binary data to float32: %w", err)
					}
					vectors = append(vectors, convertedVector)
				}
			}
		}

		totalRead += n

		// Log progress every 100k rows for larger batches
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
