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

package testinghelpers

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeRawDataset writes a dataset of train and test rows of 3 dimensions,
// where row i of a file is (i, i+0.5, -i), and 2 neighbors per test row,
// where test row i has the neighbors i and i+1.
func writeRawDataset(t *testing.T, dir string, train, test int, meta string) {
	t.Helper()
	vectors := func(rows int) []byte {
		out := make([]byte, 0, rows*3*4)
		for i := 0; i < rows; i++ {
			for _, x := range []float32{float32(i), float32(i) + 0.5, -float32(i)} {
				out = binary.LittleEndian.AppendUint32(out, math.Float32bits(x))
			}
		}
		return out
	}
	neighbors := make([]byte, 0, test*2*4)
	for i := 0; i < test; i++ {
		neighbors = binary.LittleEndian.AppendUint32(neighbors, uint32(i))
		neighbors = binary.LittleEndian.AppendUint32(neighbors, uint32(i+1))
	}

	require.NoError(t, os.WriteFile(filepath.Join(dir, "train.f32"), vectors(train), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.f32"), vectors(test), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "neighbors.i32"), neighbors, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "meta.txt"), []byte(meta), 0o644))
}

func TestReadRawDataset(t *testing.T) {
	const meta = "distances 4 2\nneighbors 4 2\ntest 4 3\ntrain 6 3\n"

	tests := []struct {
		name          string
		trainLimit    int
		queryLimit    int
		wantTrain     int
		wantQueries   int
		wantNeighbors bool
	}{
		{name: "everything", trainLimit: 0, queryLimit: 10, wantTrain: 6, wantQueries: 4, wantNeighbors: true},
		{name: "a limit above the train size", trainLimit: 99, queryLimit: 4, wantTrain: 6, wantQueries: 4, wantNeighbors: true},
		{name: "fewer queries", trainLimit: 0, queryLimit: 2, wantTrain: 6, wantQueries: 2, wantNeighbors: true},
		{
			// the shipped neighbors are computed over the full train set
			name: "a truncated train set drops the ground truth", trainLimit: 5, queryLimit: 4,
			wantTrain: 5, wantQueries: 4, wantNeighbors: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeRawDataset(t, dir, 6, 4, meta)

			data, err := ReadRawDataset(dir, tt.trainLimit, tt.queryLimit)
			require.NoError(t, err)
			assert.Equal(t, filepath.Base(dir), data.Name)
			assert.Equal(t, 3, data.Dims)
			require.Len(t, data.Train, tt.wantTrain)
			require.Len(t, data.Queries, tt.wantQueries)

			for i, vec := range data.Train {
				assert.Equal(t, []float32{float32(i), float32(i) + 0.5, -float32(i)}, vec)
				assert.Equal(t, 3, cap(vec), "a vector must not be appendable into the next one")
			}
			for i, vec := range data.Queries {
				assert.Equal(t, []float32{float32(i), float32(i) + 0.5, -float32(i)}, vec)
			}

			if !tt.wantNeighbors {
				assert.Nil(t, data.Neighbors)
				return
			}
			require.Len(t, data.Neighbors, tt.wantQueries)
			for i, ids := range data.Neighbors {
				assert.Equal(t, []uint64{uint64(i), uint64(i + 1)}, ids)
			}
		})
	}
}

func TestReadRawDatasetRejectsBrokenDatasets(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, dir string)
	}{
		{name: "no meta.txt", setup: func(t *testing.T, dir string) {}},
		{
			name: "meta.txt without the train set",
			setup: func(t *testing.T, dir string) {
				writeRawDataset(t, dir, 6, 4, "test 4 3\nneighbors 4 2\n")
			},
		},
		{
			name: "train and test of different dimensions",
			setup: func(t *testing.T, dir string) {
				writeRawDataset(t, dir, 6, 4, "train 6 3\ntest 4 2\nneighbors 4 2\n")
			},
		},
		{
			name: "a train file shorter than meta.txt says",
			setup: func(t *testing.T, dir string) {
				writeRawDataset(t, dir, 6, 4, "train 60 3\ntest 4 3\nneighbors 4 2\n")
			},
		},
		{
			name: "a missing test file",
			setup: func(t *testing.T, dir string) {
				writeRawDataset(t, dir, 6, 4, "train 6 3\ntest 4 3\nneighbors 4 2\n")
				require.NoError(t, os.Remove(filepath.Join(dir, "test.f32")))
			},
		},
		{
			name: "a neighbors file shorter than meta.txt says",
			setup: func(t *testing.T, dir string) {
				writeRawDataset(t, dir, 6, 4, "train 6 3\ntest 4 3\nneighbors 4 20\n")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tt.setup(t, dir)
			_, err := ReadRawDataset(dir, 0, 4)
			require.Error(t, err)
		})
	}
}

// A dataset larger than one decode chunk must come out whole.
func TestReadRawVectorsAcrossChunks(t *testing.T) {
	const rows, dims = 3, 700_000 // 2.1M values, two chunks of 1M and a rest
	raw := make([]byte, rows*dims*4)
	for i := 0; i < rows*dims; i++ {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(float32(i%9973)))
	}
	path := filepath.Join(t.TempDir(), "train.f32")
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	vectors, err := readRawVectors(path, rows, dims)
	require.NoError(t, err)
	require.Len(t, vectors, rows)
	for r, vec := range vectors {
		require.Len(t, vec, dims)
		for _, c := range []int{0, 1, dims / 2, dims - 1} {
			require.Equalf(t, float32((r*dims+c)%9973), vec[c], "row %d column %d", r, c)
		}
	}
}
