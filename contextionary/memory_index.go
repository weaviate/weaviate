/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package contextionary

import (
	"fmt"
	"sort"

	annoy "github.com/creativesoftwarefdn/weaviate/contextionary/annoyindex"
)

type MemoryIndex struct {
	dimensions int
	words      []string
	knn        annoy.AnnoyIndex
}

// Return the number of items that is stored in the index.
func (mi *MemoryIndex) GetNumberOfItems() int {
	return len(mi.words)
}

// Returns the length of the used vectors.
func (mi *MemoryIndex) GetVectorLength() int {
	return mi.dimensions
}

// Look up a word, return an index.
// Perform binary search.
func (mi *MemoryIndex) WordToItemIndex(word string) ItemIndex {
	for idx, w := range mi.words {
		if word == w {
			return ItemIndex(idx)
		}
	}

	return -1
}

// Based on an index, return the assosiated word.
func (mi *MemoryIndex) ItemIndexToWord(item ItemIndex) (string, error) {
	if item >= 0 && int(item) <= len(mi.words) {
		return mi.words[item], nil
	} else {
		return "", fmt.Errorf("Index out of bounds")
	}
}

// Get the vector of an item index.
func (mi *MemoryIndex) GetVectorForItemIndex(item ItemIndex) (*Vector, error) {
	if item >= 0 && int(item) <= len(mi.words) {
		var floats []float32
		mi.knn.GetItem(int(item), &floats)

		return &Vector{floats}, nil
	} else {
		return nil, fmt.Errorf("Index out of bounds")
	}
}

// Compute the distance between two items.
func (mi MemoryIndex) GetDistance(a ItemIndex, b ItemIndex) (float32, error) {
	if a >= 0 && b >= 0 && int(a) <= len(mi.words) && int(b) <= len(mi.words) {
		return mi.knn.GetDistance(int(a), int(b)), nil
	} else {
		return 0, fmt.Errorf("Index out of bounds")
	}
}

// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (mi *MemoryIndex) GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error) {
	if item >= 0 && int(item) <= len(mi.words) {
		var items []int
		var distances []float32

		mi.knn.GetNnsByItem(int(item), n, k, &items, &distances)

		var indices []ItemIndex = make([]ItemIndex, len(items))
		for i, x := range items {
			indices[i] = ItemIndex(x)
		}

		return indices, distances, nil
	} else {
		return nil, nil, fmt.Errorf("Index out of bounds")
	}
}

// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (mi *MemoryIndex) GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error) {
	if len(vector.vector) == mi.dimensions {
		var items []int
		var distances []float32

		mi.knn.GetNnsByVector(vector.vector, n, k, &items, &distances)

		var indices []ItemIndex = make([]ItemIndex, len(items))
		for i, x := range items {
			indices[i] = ItemIndex(x)
		}

		return indices, distances, nil
	} else {
		return nil, nil, fmt.Errorf("Wrong vector length provided")
	}
}

// The rest of this file concerns itself with building the Memory Index.
// This is done from the MemoryIndexBuilder struct.

type MemoryIndexBuilder struct {
	dimensions   int
	word_vectors mib_pairs
}

type mib_pair struct {
	word   string
	vector Vector
}

// Define custom type, and implement functions required for sort.Sort.
type mib_pairs []mib_pair

func (a mib_pairs) Len() int           { return len(a) }
func (a mib_pairs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a mib_pairs) Less(i, j int) bool { return a[i].word < a[j].word }

// Construct a new builder.
func InMemoryBuilder(dimensions int) *MemoryIndexBuilder {
	mib := MemoryIndexBuilder{
		dimensions:   dimensions,
		word_vectors: make([]mib_pair, 0),
	}

	return &mib
}

// Add a word and it's vector to the builder.
func (mib *MemoryIndexBuilder) AddWord(word string, vector Vector) {
	wv := mib_pair{word: word, vector: vector}
	mib.word_vectors = append(mib.word_vectors, wv)
}

// Build an efficient lookup iddex from the builder.
func (mib *MemoryIndexBuilder) Build(trees int) *MemoryIndex {
	mi := MemoryIndex{
		dimensions: mib.dimensions,
		words:      make([]string, 0),
		knn:        annoy.NewAnnoyIndexEuclidean(mib.dimensions),
	}

	// First sort the words; this way we can do binary search on the words.
	sort.Sort(mib.word_vectors)

	// Then fill up the data in the MemoryIndex
	for i, pair := range mib.word_vectors {
		mi.words = append(mi.words, pair.word)
		mi.knn.AddItem(i, pair.vector.vector)
	}

	// And instruct Annoy to build it's index
	mi.knn.Build(trees)

	return &mi
}
