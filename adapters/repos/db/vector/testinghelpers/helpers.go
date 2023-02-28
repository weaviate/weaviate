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

package testinghelpers

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

type DistanceFunction func([]float32, []float32) float32

func int32FromBytes(bytes []byte) int {
	return int(binary.LittleEndian.Uint32(bytes))
}

func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}

func readSiftFloat(file string, maxObjects int, vectorLengthFloat int) [][]float32 {
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
	objects := make([][]float32, maxObjects)
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
		vectorFloat := []float32{}
		for j := 0; j < vectorLengthFloat; j++ {
			start := (j + 1) * bytesPerF // first bytesPerF are length of vector
			vectorFloat = append(vectorFloat, float32FromBytes(vectorBytes[start:start+bytesPerF]))
		}
		objects[i] = vectorFloat

		if i >= maxObjects-1 {
			break
		}
	}

	return objects
}

func ReadSiftVecsFrom(path string, size int, dimensions int) [][]float32 {
	fmt.Printf("generating %d vectors...", size)
	vectors := readSiftFloat(path, size, dimensions)
	fmt.Printf(" done\n")
	return vectors
}

func RandomVecs(size int, queriesSize int, dimensions int) ([][]float32, [][]float32) {
	fmt.Printf("generating %d vectors...\n", size+queriesSize)
	vectors := make([][]float32, 0, size)
	queries := make([][]float32, 0, queriesSize)
	for i := 0; i < size; i++ {
		vectors = append(vectors, genVector(dimensions))
	}
	for i := 0; i < queriesSize; i++ {
		queries = append(queries, genVector(dimensions))
	}
	return vectors, queries
}

func genVector(dimensions int) []float32 {
	vector := make([]float32, 0, dimensions)
	for i := 0; i < dimensions; i++ {
		vector = append(vector, rand.Float32())
	}
	return vector
}

func Normalize(vectors [][]float32) {
	for i := range vectors {
		vectors[i] = distancer.Normalize(vectors[i])
	}
}

func ReadVecs(size int, queriesSize int, dimensions int, db string, path ...string) ([][]float32, [][]float32) {
	fmt.Printf("generating %d vectors...", size+queriesSize)
	uri := db
	if len(path) > 0 {
		uri = fmt.Sprintf("%s/%s", path[0], uri)
	}
	vectors := readSiftFloat(fmt.Sprintf("%s/%s_base.fvecs", uri, db), size, dimensions)
	queries := readSiftFloat(fmt.Sprintf("%s/%s_query.fvecs", uri, db), queriesSize, dimensions)
	fmt.Printf(" done\n")
	return vectors, queries
}

func ReadQueries(queriesSize int) [][]float32 {
	fmt.Printf("generating %d vectors...", queriesSize)
	queries := readSiftFloat("sift/sift_query.fvecs", queriesSize, 128)
	fmt.Printf(" done\n")
	return queries
}

func BruteForce(vectors [][]float32, query []float32, k int, distance DistanceFunction) []uint64 {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	ssdhelpers.Concurrently(uint64(len(vectors)), func(i uint64) {
		dist := distance(query, vectors[i])
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	})

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	if len(distances) < k {
		k = len(distances)
	}

	out := make([]uint64, k)
	for i := 0; i < k; i++ {
		out[i] = distances[i].index
	}

	return out
}

func BuildTruths(queries_size int, vectors_size int, queries [][]float32, vectors [][]float32, k int, distance DistanceFunction, path ...string) [][]uint64 {
	uri := "sift/sift_truths%d.%d.gob"
	if len(path) > 0 {
		uri = fmt.Sprintf("%s/%s", path[0], uri)
	}
	fileName := fmt.Sprintf(uri, k, vectors_size)
	truths := make([][]uint64, queries_size)

	if _, err := os.Stat(fileName); err == nil {
		return loadTruths(fileName, queries_size, k)
	}

	ssdhelpers.Concurrently(uint64(len(queries)), func(i uint64) {
		truths[i] = BruteForce(vectors, queries[i], k, distance)
	})

	f, err := os.Create(fileName)
	if err != nil {
		panic(errors.Wrap(err, "Could not open file"))
	}

	defer f.Close()
	enc := gob.NewEncoder(f)
	err = enc.Encode(truths)
	if err != nil {
		panic(errors.Wrap(err, "Could not encode truths"))
	}
	return truths
}

func loadTruths(fileName string, queries_size int, k int) [][]uint64 {
	f, err := os.Open(fileName)
	if err != nil {
		panic(errors.Wrap(err, "Could not open truths file"))
	}
	defer f.Close()

	truths := make([][]uint64, queries_size)
	cDec := gob.NewDecoder(f)
	err = cDec.Decode(&truths)
	if err != nil {
		panic(errors.Wrap(err, "Could not decode truths"))
	}
	return truths
}

func MatchesInLists(control []uint64, results []uint64) uint64 {
	desired := map[uint64]struct{}{}
	for _, relevant := range control {
		desired[relevant] = struct{}{}
	}

	var matches uint64
	for _, candidate := range results {
		_, ok := desired[candidate]
		if ok {
			matches++
		}
	}

	return matches
}
