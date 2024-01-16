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

//go:build ignore
// +build ignore

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func main() {
	dimensions := 256
	size := 10000
	queries := 1000

	vectors := make([][]float32, size)
	queryVectors := make([][]float32, queries)
	truths := make([][]uint64, queries)

	fmt.Printf("generating %d vectors", size)
	for i := 0; i < size; i++ {
		vector := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			vector[j] = rand.Float32()
		}
		vectors[i] = Normalize(vector)

	}
	fmt.Printf("done\n")

	fmt.Printf("generating %d search queries", queries)
	for i := 0; i < queries; i++ {
		queryVector := make([]float32, dimensions)
		for j := 0; j < dimensions; j++ {
			queryVector[j] = rand.Float32()
		}
		queryVectors[i] = Normalize(queryVector)
	}
	fmt.Printf("done\n")

	fmt.Printf("defining truth through brute force")

	k := 10
	for i, query := range queryVectors {
		truths[i] = bruteForce(vectors, query, k)
	}

	vectorsJSON, _ := json.Marshal(vectors)
	queriesJSON, _ := json.Marshal(queryVectors)
	truthsJSON, _ := json.Marshal(truths)

	ioutil.WriteFile("recall_vectors.json", vectorsJSON, 0o644)
	ioutil.WriteFile("recall_queries.json", queriesJSON, 0o644)
	ioutil.WriteFile("recall_truths.json", truthsJSON, 0o644)
}

func Normalize(v []float32) []float32 {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}

	return v
}

func bruteForce(vectors [][]float32, query []float32, k int) []uint64 {
	type distanceAndIndex struct {
		distance float32
		index    uint64
	}

	distances := make([]distanceAndIndex, len(vectors))

	for i, vec := range vectors {
		dist, _, _ := distancer.NewCosineDistanceProvider().SingleDist(query, vec)
		distances[i] = distanceAndIndex{
			index:    uint64(i),
			distance: dist,
		}
	}

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
