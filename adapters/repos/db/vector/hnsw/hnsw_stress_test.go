//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	vectorSize         = 1024
	vectorsToAdd       = 100
	parallelGoroutines = 100
)

func idVector(ctx context.Context, id uint64) ([]float32, error) {
	vector := make([]float32, vectorSize)
	for i := 0; i < vectorSize; i++ {
		vector[i] = float32(id)
	}
	return vector, nil
}

func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func int32FromBytes(bytes []byte) int {
	return int(binary.LittleEndian.Uint32(bytes))
}

func BenchmarkHnswStress(b *testing.B) {
	siftFile := "siftsmall/siftsmall_base.fvecs"
	if _, err := os.Stat(siftFile); err != nil {
		b.Skip("Sift data needs to be present")
	}
	wg := sync.WaitGroup{}
	vectors := readSiftFloat(siftFile, parallelGoroutines*vectorsToAdd)

	for n := 0; n < b.N; n++ {
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "unittest",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk:      idVector,
		}, ent.UserConfig{
			MaxConnections: 30,
			EFConstruction: 60,
		}, cyclemanager.NewNoop())
		require.Nil(b, err)
		for k := 0; k < parallelGoroutines; k++ {
			wg.Add(1)
			goroutineIndex := k * vectorsToAdd
			go func() {
				for i := 0; i < vectorsToAdd; i++ {

					err := index.Add(uint64(goroutineIndex+i), vectors[goroutineIndex+i])
					require.Nil(b, err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func readSiftFloat(file string, maxObjects int) [][]float32 {
	var vectors [][]float32

	f, err := os.Open(file)
	if err != nil {
		panic(errors.Wrap(err, "Could not open SIFT file"))
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		panic(errors.Wrap(err, "Could not get SIFT file properties"))
	}
	fileSize := fi.Size()
	if fileSize < 1000000 {
		panic("The file is only " + fmt.Sprint(fileSize) + " bytes long. Did you forgot to install git lfs?")
	}

	// The sift data is a binary file containing floating point vectors
	// For each entry, the first 4 bytes is the length of the vector (in number of floats, not in bytes)
	// which is followed by the vector data with vector length * 4 bytes.
	// |-length-vec1 (4bytes)-|-Vec1-data-(4*length-vector-1 bytes)-|-length-vec2 (4bytes)-|-Vec2-data-(4*length-vector-2 bytes)-|
	// The vector length needs to be converted from bytes to int
	// The vector data needs to be converted from bytes to float
	// Note that the vector entries are of type float but are integer numbers eg 2.0
	bytesPerF := 4
	vectorLengthFloat := 128
	vectorBytes := make([]byte, bytesPerF+vectorLengthFloat*bytesPerF)
	for i := 0; i >= 0; i++ {
		_, err = f.Read(vectorBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if int32FromBytes(vectorBytes[0:bytesPerF]) != vectorLengthFloat {
			panic("Each vector must have 128 entries.")
		}
		vectorFloat := make([]float32, 0, vectorLengthFloat)
		for j := 0; j < vectorLengthFloat; j++ {
			start := (j + 1) * bytesPerF // first bytesPerF are length of vector
			vectorFloat = append(vectorFloat, float32FromBytes(vectorBytes[start:start+bytesPerF]))
		}

		vectors = append(vectors, vectorFloat)

		if i >= maxObjects {
			break
		}
	}
	if len(vectors) < maxObjects {
		panic("Could not load all elements.")
	}

	return vectors
}
