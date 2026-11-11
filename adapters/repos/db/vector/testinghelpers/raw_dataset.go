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
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/usecases/byteops"
)

// RawDataset is an ANN benchmark dataset exported as raw little-endian files:
// a directory with train.f32 and test.f32 (float32 rows), neighbors.i32
// (int32 ids of the exact nearest train rows of every test row, nearest
// first) and a meta.txt that gives their shapes, one "<name> <rows> <cols>"
// line per file ("train 990000 1536", "test 10000 1536", "neighbors 10000
// 100").
type RawDataset struct {
	Name string
	Dims int
	// TrainRows is the size of the train set of the dataset, whatever part
	// of it was read.
	TrainRows int
	Train     [][]float32
	Queries   [][]float32
	// Neighbors holds the shipped ground truth of every query. It is nil
	// when the train set was truncated, which invalidates it.
	Neighbors [][]uint64
}

// ReadRawDataset reads the first trainLimit train rows (all of them if the
// limit is not positive) and the first queryLimit test rows of the dataset in
// dir.
func ReadRawDataset(dir string, trainLimit, queryLimit int) (*RawDataset, error) {
	return readRawDataset(dir, trainLimit, queryLimit, true)
}

// ReadRawQueries reads the dataset in dir without its train vectors: the
// first queryLimit test rows, their ground truth and the shape of the train
// set. Train is left nil, for ReadRawTrainChunks to read.
func ReadRawQueries(dir string, queryLimit int) (*RawDataset, error) {
	return readRawDataset(dir, 0, queryLimit, false)
}

func readRawDataset(dir string, trainLimit, queryLimit int, withTrain bool) (*RawDataset, error) {
	shapes, err := readRawShapes(filepath.Join(dir, "meta.txt"))
	if err != nil {
		return nil, err
	}
	train, test := shapes["train"], shapes["test"]
	if train[0] == 0 || test[0] == 0 {
		return nil, errors.Errorf("%s: meta.txt must describe train and test", dir)
	}
	if train[1] != test[1] {
		return nil, errors.Errorf("%s: train has %d dimensions, test has %d", dir, train[1], test[1])
	}

	data := &RawDataset{Name: filepath.Base(dir), Dims: train[1], TrainRows: train[0]}
	rows := train[0]
	if trainLimit > 0 && trainLimit < rows {
		rows = trainLimit
	}
	if withTrain {
		if data.Train, err = readRawVectors(filepath.Join(dir, "train.f32"), rows, data.Dims); err != nil {
			return nil, err
		}
	}
	queries := min(queryLimit, test[0])
	if data.Queries, err = readRawVectors(filepath.Join(dir, "test.f32"), queries, data.Dims); err != nil {
		return nil, err
	}

	neighbors := shapes["neighbors"]
	if rows != train[0] || neighbors[0] < queries || neighbors[1] == 0 {
		return data, nil
	}
	raw := make([]byte, queries*neighbors[1]*4)
	if err := readRawInto(filepath.Join(dir, "neighbors.i32"), raw); err != nil {
		return nil, err
	}
	data.Neighbors = make([][]uint64, queries)
	for i := range data.Neighbors {
		data.Neighbors[i] = make([]uint64, neighbors[1])
		for j := range data.Neighbors[i] {
			data.Neighbors[i][j] = uint64(binary.LittleEndian.Uint32(raw[(i*neighbors[1]+j)*4:]))
		}
	}
	return data, nil
}

func readRawShapes(path string) (map[string][2]int, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	shapes := map[string][2]int{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			continue
		}
		rows, errRows := strconv.Atoi(fields[1])
		cols, errCols := strconv.Atoi(fields[2])
		if errRows == nil && errCols == nil {
			shapes[fields[0]] = [2]int{rows, cols}
		}
	}
	return shapes, scanner.Err()
}

// readRawVectors reads rows vectors into one backing array. Each returned
// vector is capped at its own length, so appending to one cannot run into the
// next.
func readRawVectors(path string, rows, dims int) ([][]float32, error) {
	if rows == 0 || dims == 0 {
		return nil, errors.Errorf("%s: nothing to read (%d rows of %d dimensions)", path, rows, dims)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Decoded chunk by chunk: the files are several gigabytes, too large
	// to hold a second time as bytes.
	flat := make([]float32, rows*dims)
	chunk := make([]byte, 4*min(len(flat), 1<<20))
	for done := 0; done < len(flat); {
		n := min(len(flat)-done, len(chunk)/4)
		if _, err := io.ReadFull(file, chunk[:4*n]); err != nil {
			return nil, errors.Wrapf(err, "read %s", path)
		}
		byteops.CopyBytesToSlice(flat[done:done+n], chunk[:4*n])
		done += n
	}

	vectors := make([][]float32, rows)
	for i := range vectors {
		vectors[i] = flat[i*dims : (i+1)*dims : (i+1)*dims]
	}
	return vectors, nil
}

// ReadRawTrainChunks hands the train vectors of the dataset in dir to fn in
// order, chunkRows at a time and fewer in the last chunk: first is the
// position of the first vector of the chunk. Every chunk has a backing array
// of its own, so fn may keep it, and dropping it frees it.
//
// It is for a train set that does not fit in memory twice: an index that
// copies what it is given can be fed this way without the caller holding the
// whole set next to it.
func ReadRawTrainChunks(dir string, chunkRows int, fn func(first int, chunk [][]float32) error) error {
	shapes, err := readRawShapes(filepath.Join(dir, "meta.txt"))
	if err != nil {
		return err
	}
	rows, dims := shapes["train"][0], shapes["train"][1]
	path := filepath.Join(dir, "train.f32")
	if rows == 0 || dims == 0 {
		return errors.Errorf("%s: nothing to read (%d rows of %d dimensions)", path, rows, dims)
	}
	if chunkRows < 1 {
		return errors.Errorf("%s: cannot read in chunks of %d rows", path, chunkRows)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	raw := make([]byte, 4*dims*min(rows, chunkRows))
	for first := 0; first < rows; first += chunkRows {
		n := min(chunkRows, rows-first)
		if _, err := io.ReadFull(file, raw[:4*dims*n]); err != nil {
			return errors.Wrapf(err, "read %s", path)
		}
		flat := make([]float32, n*dims)
		byteops.CopyBytesToSlice(flat, raw[:4*dims*n])
		chunk := make([][]float32, n)
		for i := range chunk {
			chunk[i] = flat[i*dims : (i+1)*dims : (i+1)*dims]
		}
		if err := fn(first, chunk); err != nil {
			return err
		}
	}
	return nil
}

func readRawInto(path string, into []byte) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.ReadFull(file, into); err != nil {
		return errors.Wrapf(err, "read %d bytes of %s", len(into), path)
	}
	return nil
}
