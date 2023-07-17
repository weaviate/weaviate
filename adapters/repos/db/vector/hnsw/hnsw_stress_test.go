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
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	vectorSize          = 128
	vectorsPerGoroutine = 100
	parallelGoroutines  = 100
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

func TestHnswStress(t *testing.T) {
	siftFile := "siftsmall/siftsmall_base.fvecs"
	if _, err := os.Stat(siftFile); err != nil {
		t.Skip("Sift data needs to be present")
	}
	vectors := readSiftFloat(siftFile, parallelGoroutines*vectorsPerGoroutine)

	t.Run("Insert and search and maybe delete", func(t *testing.T) {
		for n := 0; n < 1; n++ { // increase if you don't want to reread SIFT for every run
			wg := sync.WaitGroup{}
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
			require.Nil(t, err)
			for k := 0; k < parallelGoroutines; k++ {
				wg.Add(2)
				goroutineIndex := k * vectorsPerGoroutine
				go func() {
					for i := 0; i < vectorsPerGoroutine; i++ {

						err := index.Add(uint64(goroutineIndex+i), vectors[goroutineIndex+i])
						require.Nil(t, err)
					}
					wg.Done()
				}()

				go func() {
					for i := 0; i < vectorsPerGoroutine; i++ {
						for j := 0; j < 5; j++ { // try a couple of times to delete if found
							_, dists, err := index.SearchByVector(vectors[goroutineIndex+i], 0, nil)
							require.Nil(t, err)

							if len(dists) > 0 && dists[0] == 0 {
								err := index.Delete(uint64(goroutineIndex + i))
								require.Nil(t, err)
								break
							} else {
								continue
							}
						}
					}
					wg.Done()
				}()
			}
			wg.Wait()
		}
	})

	t.Run("Insert and delete", func(t *testing.T) {
		for i := 0; i < 1; i++ { // increase if you don't want to reread SIFT for every run
			wg := sync.WaitGroup{}
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
			require.Nil(t, err)
			for k := 0; k < parallelGoroutines; k++ {
				wg.Add(1)
				goroutineIndex := k * vectorsPerGoroutine
				go func() {
					for i := 0; i < vectorsPerGoroutine; i++ {

						err := index.Add(uint64(goroutineIndex+i), vectors[goroutineIndex+i])
						require.Nil(t, err)
						err = index.Delete(uint64(goroutineIndex + i))
						require.Nil(t, err)

					}
					wg.Done()
				}()

			}
			wg.Wait()

		}
	})

	t.Run("Concurrent deletes", func(t *testing.T) {
		for i := 0; i < 10; i++ { // increase if you don't want to reread SIFT for every run
			wg := sync.WaitGroup{}

			cycle := cyclemanager.NewMulti(cyclemanager.NewFixedIntervalTicker(time.Nanosecond))
			cycle.Start()
			index, err := New(Config{
				RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
				ID:                    "unittest",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				VectorForIDThunk:      idVector,
			}, ent.UserConfig{
				MaxConnections: 30,
				EFConstruction: 60,
			}, cycle)
			require.Nil(t, err)
			deleteIds := make([]uint64, 50)
			for j := 0; j < len(deleteIds); j++ {
				err = index.Add(uint64(j), vectors[j])
				require.Nil(t, err)
				deleteIds[j] = uint64(j)
			}
			wg.Add(2)

			go func() {
				err := index.Delete(deleteIds[25:]...)
				require.Nil(t, err)
				wg.Done()
			}()
			go func() {
				err := index.Delete(deleteIds[:24]...)
				require.Nil(t, err)
				wg.Done()
			}()

			wg.Wait()

			time.Sleep(time.Microsecond * 100)
			index.Lock()
			require.NotNil(t, index.nodes[24])
			index.Unlock()

		}
	})
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
	vectorBytes := make([]byte, bytesPerF+vectorSize*bytesPerF)
	for i := 0; i >= 0; i++ {
		_, err = f.Read(vectorBytes)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if int32FromBytes(vectorBytes[0:bytesPerF]) != vectorSize {
			panic("Each vector must have 128 entries.")
		}
		vectorFloat := make([]float32, 0, vectorSize)
		for j := 0; j < vectorSize; j++ {
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
