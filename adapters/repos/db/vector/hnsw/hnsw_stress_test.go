//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

func idVectorSize(size int) func(ctx context.Context, id uint64) ([]float32, error) {
	return func(ctx context.Context, id uint64) ([]float32, error) {
		vector := make([]float32, size)
		for i := 0; i < size; i++ {
			vector[i] = float32(id)
		}
		return vector, nil
	}
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
	siftFile := "datasets/ann-benchmarks/siftsmall/siftsmall_base.fvecs"
	if _, err := os.Stat(siftFile); err != nil {
		if !*download {
			t.Skip(`Sift data needs to be present.
Run test with -download to automatically download the dataset.
Ex: go test -v -run TestHnswStress . -download
`)
		}
		downloadDatasetFile(t, siftFile)
	}
	vectors := readSiftFloat(siftFile, parallelGoroutines*vectorsPerGoroutine)

	t.Run("Insert and search and maybe delete", func(t *testing.T) {
		for n := 0; n < 1; n++ { // increase if you don't want to reread SIFT for every run
			wg := sync.WaitGroup{}
			index := createEmptyHnswIndexForTests(t, idVector)
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
			index := createEmptyHnswIndexForTests(t, idVector)
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

			index := createEmptyHnswIndexForTests(t, idVector)
			deleteIds := make([]uint64, 50)
			for j := 0; j < len(deleteIds); j++ {
				err := index.Add(uint64(j), vectors[j])
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

	t.Run("Random operations", func(t *testing.T) {
		for i := 0; i < 1; i++ { // increase if you don't want to reread SIFT for every run
			index := createEmptyHnswIndexForTests(t, idVector)

			var inserted struct {
				sync.Mutex
				ids []uint64
				set map[uint64]struct{}
			}
			inserted.set = make(map[uint64]struct{})

			claimUnusedID := func() (uint64, bool) {
				inserted.Lock()
				defer inserted.Unlock()

				if len(inserted.ids) == len(vectors) {
					return 0, false
				}

				try := 0
				for {
					id := uint64(rand.Intn(len(vectors)))
					if _, ok := inserted.set[id]; !ok {
						inserted.ids = append(inserted.ids, id)
						inserted.set[id] = struct{}{}
						return id, true
					}

					try++
					if try > 50 {
						log.Printf("[WARN] tried %d times, retrying...\n", try)
					}
				}
			}

			getInsertedIDs := func(n int) []uint64 {
				inserted.Lock()
				defer inserted.Unlock()

				if len(inserted.ids) < n {
					return nil
				}

				if n > len(inserted.ids) {
					n = len(inserted.ids)
				}

				ids := make([]uint64, n)
				copy(ids, inserted.ids[:n])

				return ids
			}

			removeInsertedIDs := func(ids ...uint64) {
				inserted.Lock()
				defer inserted.Unlock()

				for _, id := range ids {
					delete(inserted.set, id)
					for i, insertedID := range inserted.ids {
						if insertedID == id {
							inserted.ids = append(inserted.ids[:i], inserted.ids[i+1:]...)
							break
						}
					}
				}
			}

			ops := []func(){
				// Add
				func() {
					id, ok := claimUnusedID()
					if !ok {
						return
					}

					err := index.Add(id, vectors[id])
					require.Nil(t, err)
				},
				// Delete
				func() {
					// delete 5% of the time
					if rand.Int31()%20 == 0 {
						return
					}

					ids := getInsertedIDs(rand.Intn(100) + 1)

					err := index.Delete(ids...)
					require.Nil(t, err)

					removeInsertedIDs(ids...)
				},
				// Search
				func() {
					// search 50% of the time
					if rand.Int31()%2 == 0 {
						return
					}

					id := rand.Intn(len(vectors))

					_, _, err := index.SearchByVector(vectors[id], 0, nil)
					require.Nil(t, err)
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
			defer cancel()

			g, ctx := errgroup.WithContext(ctx)

			// run parallelGoroutines goroutines
			for i := 0; i < parallelGoroutines; i++ {
				g.Go(func() error {
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						default:
							ops[rand.Intn(len(ops))]()
						}
					}
				})
			}

			g.Wait()
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
