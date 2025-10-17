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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataReaderReadNextChunk(t *testing.T) {
	// All values in the train and test dataset are set to the row index.
	// The train file contains 100 rows and the test file contains 10 rows.
	dim := 32
	k := 100

	bufferSize := 10
	trainReader, err := NewLocalDataReader("testdata/train.parquet", TrainSplit, 0, -1, bufferSize)
	if err != nil {
		t.Fatalf("Failed to load training data file: %v", err)
	}
	defer trainReader.Close()

	chunk, err := trainReader.ReadNextChunk()
	if err != nil {
		t.Fatalf("Failed to read first chunk from training data: %v", err)
	}
	require.Equal(t, TrainSplit, chunk.Split)
	require.Equal(t, 0, chunk.RowOffset)
	require.Equal(t, bufferSize, chunk.NumRows)
	require.Equal(t, uint64(5), chunk.IDs[5])
	require.Equal(t, dim, len(chunk.Vectors[5]))
	require.Equal(t, float32(5), chunk.Vectors[5][0])

	chunk, err = trainReader.ReadNextChunk()
	if err != nil {
		t.Fatalf("Failed to read next chunk from training data: %v", err)
	}
	require.Equal(t, TrainSplit, chunk.Split)
	require.Equal(t, bufferSize, chunk.RowOffset)
	require.Equal(t, bufferSize, chunk.NumRows)
	require.Equal(t, uint64(15), chunk.IDs[5])
	require.Equal(t, dim, len(chunk.Vectors[5]))
	require.Equal(t, float32(15), chunk.Vectors[5][0])

	testBufferSize := 4
	testReader, err := NewLocalDataReader("testdata/test.parquet", TestSplit, 0, -1, testBufferSize)
	if err != nil {
		t.Fatalf("Failed to load test data file: %v", err)
	}
	defer testReader.Close()

	chunk, err = testReader.ReadNextChunk()
	if err != nil {
		t.Fatalf("Failed to read first chunk from test data: %v", err)
	}
	require.Equal(t, TestSplit, chunk.Split)
	require.Equal(t, 0, chunk.RowOffset)
	require.Equal(t, testBufferSize, chunk.NumRows)
	require.Equal(t, uint64(3), chunk.IDs[3])
	require.Equal(t, dim, len(chunk.Vectors[3]))
	require.Equal(t, float32(3), chunk.Vectors[3][0])
	require.Equal(t, k, len(chunk.Neighbors[3]))
	require.Equal(t, uint64(3), chunk.Neighbors[3][0])

	chunk, err = testReader.ReadNextChunk()
	require.Nil(t, err)
	require.Equal(t, testBufferSize, chunk.NumRows)

	// Verify that we get an EOF error after reading all 10 rows of test data
	chunk, err = testReader.ReadNextChunk()
	require.True(t, errors.Is(err, io.EOF))
	require.Equal(t, 2, chunk.NumRows)
}

func TestDataReaderBounds(t *testing.T) {
	startRow := 45
	endRow := 60
	bufferSize := 10
	trainReader, err := NewLocalDataReader("testdata/train.parquet", TrainSplit, startRow, endRow, bufferSize)
	if err != nil {
		t.Fatalf("Failed to load training data file: %v", err)
	}
	defer trainReader.Close()

	chunk, err := trainReader.ReadNextChunk()
	if err != nil {
		t.Fatalf("Failed to read first chunk from training data: %v", err)
	}
	require.Equal(t, TrainSplit, chunk.Split)
	require.Equal(t, startRow, chunk.RowOffset)
	require.Equal(t, bufferSize, chunk.NumRows)
	require.Equal(t, uint64(45), chunk.IDs[0])
	require.Equal(t, float32(45), chunk.Vectors[0][0])

	chunk, err = trainReader.ReadNextChunk()
	require.True(t, errors.Is(err, io.EOF))
	require.Equal(t, 5, chunk.NumRows)
}
