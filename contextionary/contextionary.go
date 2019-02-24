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

// Package contextionary provides the toolset to add context to words.
package contextionary

// ItemIndex is an opaque type that models an index number used to identify a
// word.
type ItemIndex int

// IsPresent can be used after retrieving a word index (which does not error on
// its own), to see if the word was actually present in the contextionary.
func (i *ItemIndex) IsPresent() bool {
	return *i >= 0
}

// Contextionary is the API to decouple the K-nn interface that is needed for
// Weaviate from a concrete implementation.
type Contextionary interface {
	// Return the number of items that is stored in the index.
	GetNumberOfItems() int

	// Returns the length of the used vectors.
	GetVectorLength() int

	// Look up a word, return an index.
	// Check for presence of the index with index.IsPresent()
	WordToItemIndex(word string) ItemIndex

	// Based on an index, return the assosiated word.
	ItemIndexToWord(item ItemIndex) (string, error)

	// Get the vector of an item index.
	GetVectorForItemIndex(item ItemIndex) (*Vector, error)

	// Compute the distance between two items.
	GetDistance(a ItemIndex, b ItemIndex) (float32, error)

	// Get the n nearest neighbours of item, examining k trees.
	// Returns an array of indices, and of distances between item and the n-nearest neighbors.
	GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error)

	// Get the n nearest neighbours of item, examining k trees.
	// Returns an array of indices, and of distances between item and the n-nearest neighbors.
	GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error)

	// Sentence to index
	// Returns the index for the sentence
	SentenceToItemIndex(sentence string) (Vector, error)
}
