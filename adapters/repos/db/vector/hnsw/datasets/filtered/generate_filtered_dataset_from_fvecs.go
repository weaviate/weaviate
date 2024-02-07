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

//go:build ignore
// +build ignore

package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Vector struct {
	ID     int       `json:"id"`
	Vector []float32 `json:"vector"`
}

type Filters struct {
	ID        int         `json:"id"`
	FilterMap map[int]int `json:"filterMap"`
}

type VecWithFilters struct {
	ID        int         `json:"id"`
	Vector    []float32   `json:"vector"`
	FilterMap map[int]int `json:"filterMap"`
}

type GroundTruth struct {
	QueryID int   `json:"queryID"`
	Truths  []int `json:"truths"`
}

func main() {
	// CLI flags
	numVectors := flag.Int("numVectors", 100_000, "Number of vectors to process")
	majorityPct := flag.Float64("majorityPct", 95.0, "Minority filter percentage of the dataset")
	//vectorDimension := flag.Int("vectorDim", 128, "Dimension of the vectors")
	//filePath := flag.String("DataPath", "./sift-data/sift_base.fvecs", "Path to the data file")

	flag.Parse()

	numLabels := "2"                                                 // ToDo extend to multiple labels
	majorityPct_str := strconv.FormatFloat(*majorityPct, 'f', 1, 64) // used for save path
	majorityPct_str = strings.ReplaceAll(majorityPct_str, ".", "_")  // prefer e.g. `95_0` save path

	// Read base vectors from file
	vectors := ReadSiftVecsFrom("./sift-data/sift_base.fvecs", *numVectors, 128)

	saveIndexVectors := make([]Vector, len(vectors))
	saveIndexFilters := make([]Filters, len(vectors))
	indexForBruteForce := make([]VecWithFilters, len(vectors))

	majority_pct := *majorityPct / 100.0
	majority_cutoff := int(10_000 * majority_pct)

	for jdx, vector := range vectors {
		nodeFilterMap := make(map[int]int)
		// ToDo -- extend to K filters, with a parameterized filter distribution (power-law)
		hash := jdx % 10_000
		if hash < majority_cutoff {
			nodeFilterMap[0] = 0
		} else {
			nodeFilterMap[0] = 1
		}
		indexForBruteForce[jdx] = VecWithFilters{
			ID:        jdx,
			FilterMap: nodeFilterMap,
			Vector:    vector,
		}
		saveIndexVectors[jdx] = Vector{
			ID:     jdx,
			Vector: vector,
		}
		saveIndexFilters[jdx] = Filters{
			ID:        jdx,
			FilterMap: nodeFilterMap,
		}
	}

	saveNumVectors := map[int]string{
		100000:  "100K",
		1000000: "1M",
	}
	saveIndexVectorsJSON, _ := json.Marshal(saveIndexVectors)
	index_save_path := "indexVectors-" + saveNumVectors[*numVectors] + ".json"
	ioutil.WriteFile(index_save_path, saveIndexVectorsJSON, 0o644)
	index_with_filters_save_path := "indexFilters-" + saveNumVectors[*numVectors] + "-" + numLabels + "-" + majorityPct_str + ".json"
	saveIndexFiltersJSON, _ := json.Marshal(saveIndexFilters)
	ioutil.WriteFile(index_with_filters_save_path, saveIndexFiltersJSON, 0o644)

	// Read the query vectors from files
	_, queryVectors := ReadVecs(*numVectors, 10_000, 128, "sift")

	saveQueryVectors := make([]Vector, len(queryVectors))
	saveQueryFilters := make([]Filters, len(queryVectors))

	groundTruths := make([]GroundTruth, len(queryVectors))

	fmt.Println("Brute forcing...")
	fmt.Println(len(queryVectors))

	workerCount := runtime.GOMAXPROCS(0)
	jobsForWorker := make([][]VecWithFilters, workerCount)
	for i, queryVector := range queryVectors {
		workerID := i % workerCount
		queryFilters := make(map[int]int)
		queryHash := i % 10_000
		if queryHash < majority_cutoff {
			queryFilters[0] = 0
		} else {
			queryFilters[0] = 1
		}
		saveQueryVectors[i] = Vector{
			ID:     i,
			Vector: queryVector,
		}
		saveQueryFilters[i] = Filters{
			ID:        i,
			FilterMap: queryFilters,
		}
		queryVectorWithFilters := VecWithFilters{i, queryVector, queryFilters}
		jobsForWorker[workerID] = append(jobsForWorker[workerID], queryVectorWithFilters)
	}

	wg := &sync.WaitGroup{}
	mutex := &sync.Mutex{}
	before := time.Now()
	for workerID, jobs := range jobsForWorker {
		wg.Add(1)
		go func(workerID int, myJobs []VecWithFilters) {
			defer wg.Done()
			for i, vecWithFilters := range myJobs {
				originalIndex := (i * workerCount) + workerID
				nearestNeighbors := calculateNearestNeighborsWithFilters(vecWithFilters.Vector, indexForBruteForce, 100, vecWithFilters.FilterMap)
				newGroundTruthJSON := GroundTruth{
					QueryID: originalIndex,
					Truths:  nearestNeighbors,
				}
				mutex.Lock()
				groundTruths[originalIndex] = newGroundTruthJSON
				mutex.Unlock()
			}
		}(workerID, jobs)
	}
	wg.Wait()
	fmt.Printf("Brute forcing took %s \n", time.Since(before))
	fmt.Printf("Saving...\n")
	saveQueryVectorsJSON, _ := json.Marshal(saveQueryVectors)
	query_vectors_save_path := "queryVectors-" + saveNumVectors[*numVectors] + ".json"
	ioutil.WriteFile(query_vectors_save_path, saveQueryVectorsJSON, 0o644)
	query_vectors_with_filters_save_path := "queryFilters-" + saveNumVectors[*numVectors] + "-" + numLabels + "-" + majorityPct_str + ".json"
	saveQueryFiltersJSON, _ := json.Marshal(saveQueryFilters)
	ioutil.WriteFile(query_vectors_with_filters_save_path, saveQueryFiltersJSON, 0o644)

	// Save all nearest neighbors to a JSON file
	saveGroundTruthsJSON, _ := json.Marshal(groundTruths)
	ground_truth_save_path := "filtered-recall-truths-" + saveNumVectors[*numVectors] + "-" + numLabels + "-" + majorityPct_str + ".json"
	ioutil.WriteFile(ground_truth_save_path, saveGroundTruthsJSON, 0o644)

	fmt.Print("\n Finished.")
}

// Function to calculate nearest neighbors of a vector using brute force
func calculateNearestNeighborsWithFilters(query []float32, indexVectors []VecWithFilters, numNeighbors int, queryFilters map[int]int) []int {
	filteredIndex := make([]VecWithFilters, 0)
	for _, v := range indexVectors {
		matches := true
		for queryFilterKey, queryFilterValue := range queryFilters {
			if value, exists := v.FilterMap[queryFilterKey]; !exists || value != queryFilterValue {
				matches = false
				break
			}
		}
		if matches {
			filteredIndex = append(filteredIndex, v)
		}
	}
	//
	type DistanceIndex struct {
		Distance float32
		Index    int
	}
	distances := make([]DistanceIndex, len(filteredIndex))

	// Compute the distance from the query to each vector
	for i, v := range filteredIndex {
		distances[i] = DistanceIndex{
			Distance: BFeuclideanDistance(query, v.Vector),
			Index:    v.ID,
		}
	}

	// Sort the distances
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].Distance < distances[j].Distance
	})

	// Extract the indices of the numNeighbors nearest neighbors
	neighbors := make([]int, numNeighbors)

	i := 0
	for i < numNeighbors {
		neighbors[i] = distances[i].Index
		i++
	}

	return neighbors
}

// Function to calculate the Euclidean distance between two vectors
func BFeuclideanDistance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return float32(math.Sqrt(float64(sum)))
}

// Function to write integer vectors to a SIFT formatted binary file
func writeSiftIVecsToFile(filename string, vectors [][]int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, vec := range vectors {
		err = writeSiftInt(file, vec)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeSiftInt(w io.Writer, vector []int) error {
	// Convert vector to []int32
	vectorInt32 := make([]int32, len(vector))
	for i, v := range vector {
		vectorInt32[i] = int32(v)
	}

	// Write vector dimension
	if err := binary.Write(w, binary.LittleEndian, int32(len(vectorInt32))); err != nil {
		return err
	}

	// Write vector
	if err := binary.Write(w, binary.LittleEndian, vectorInt32); err != nil {
		return err
	}

	return nil
}

// Function to read SIFT formatted vector data from a given path
func ReadSiftVecsFrom(path string, size int, dimensions int) [][]float32 {
	// print progress
	fmt.Printf("generating %d vectors...", size)

	// read the vectors
	vectors := readSiftFloat(path, size, dimensions)

	// print completion
	fmt.Printf(" done\n")

	// return the vectors
	return vectors
}

// Function to read base and query vector data from a given path
func ReadVecs(size int, queriesSize int, dimensions int, db string, path ...string) ([][]float32, [][]float32) {
	// print progress
	fmt.Printf("generating %d vectors...", size+queriesSize)

	// set the base uri as db
	uri := db

	// if a path is provided, prepend it to uri
	if len(path) > 0 {
		uri = fmt.Sprintf("%s/%s", path[0], uri)
	}

	// read base vectors
	vectors := readSiftFloat(fmt.Sprintf("sift-data/%s_base.fvecs", db), size, dimensions)

	// read query vectors
	queries := readSiftFloat(fmt.Sprintf("sift-data/%s_query.fvecs", db), queriesSize, dimensions)

	// print completion
	fmt.Printf(" done\n")

	// return vectors and queries
	return vectors, queries
}

// Function to read SIFT formatted vector data from a given binary file
func readSiftFloat(file string, maxObjects int, vectorLengthFloat int) [][]float32 {
	// open the file
	f, err := os.Open(file)

	// ensure file gets closed after the function exits
	defer f.Close()

	// check for file open error
	if err != nil {
		panic(err)
	}

	// Allocate memory for objects and vectorBytes
	objects := make([][]float32, maxObjects)
	vectorBytes := make([]byte, 4+vectorLengthFloat*4)

	// read the vectors from the file
	for i := 0; i >= 0; i++ {
		_, err = f.Read(vectorBytes)

		// break the loop if we have reached end of file
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		// check if the vector length matches expected length
		if int32FromBytes(vectorBytes[0:4]) != vectorLengthFloat {
			panic("Each vector must have 128 entries.")
		}

		// read each float from the vector
		vectorFloat := make([]float32, vectorLengthFloat)
		for j := 0; j < vectorLengthFloat; j++ {
			start := (j + 1) * 4 // first 4 bytes are length of vector
			vectorFloat[j] = float32FromBytes(vectorBytes[start : start+4])
		}

		// save the vector
		objects[i] = vectorFloat

		// break the loop if we have reached maximum number of objects
		if i >= maxObjects-1 {
			break
		}
	}

	// return the objects read from file
	return objects
}

// Function to convert a byte slice to int
func int32FromBytes(bytes []byte) int {
	return int(binary.LittleEndian.Uint32(bytes))
}

// Function to convert a byte slice to float32
func float32FromBytes(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	float := math.Float32frombits(bits)
	return float
}
