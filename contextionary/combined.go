/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package contextionary

import (
	"fmt"
	"sort"
)

type CombinedIndex struct {
	indices       []combinedIndex
	total_size    int
	vector_length int
}

type combinedIndex struct {
	offset int
	size   int
	index  *Contextionary
}

// Combine multiple indices, present them as one.
// It assumes that each index stores unique words
func CombineVectorIndices(indices []Contextionary) (*CombinedIndex, error) {
	// We join the ItemIndex spaces the indivual indices, by
	// offsetting the 2nd ItemIndex with len(indices[0]),
	// the 3rd ItemIndex space with len(indices[0]) + len(indices[1]), etc.

	if len(indices) < 2 {
		return nil, fmt.Errorf("Less than two vector indices provided!")
	}

	combined_indices := make([]combinedIndex, len(indices))

	var offset int = 0

	vector_length := indices[0].GetVectorLength()

	for i := 0; i < len(indices); i++ {
		size := indices[i].GetNumberOfItems()

		combined_indices[i] = combinedIndex{
			offset: offset,
			size:   size,
			index:  &indices[i],
		}

		offset += size

		my_length := indices[i].GetVectorLength()

		if my_length != vector_length {
			return nil, fmt.Errorf("vector length not equal")
		}
	}

	return &CombinedIndex{indices: combined_indices, total_size: offset, vector_length: vector_length}, nil
}

// Verify that all the indices are disjoint
// Returns nil on success, an error if the words in the indices are not disjoint.
func (ci *CombinedIndex) VerifyDisjoint() error {
	for index_i, item_i := range ci.indices {
		for i := ItemIndex(0); int(i) < item_i.size; i++ {
			word, err := (*item_i.index).ItemIndexToWord(i)
			if err != nil {
				panic("should not happen; this index should always be accessible")
			}

			for index_j, item_j := range ci.indices {
				if index_i != index_j {
					result := (*(item_j.index)).WordToItemIndex(word)
					if result.IsPresent() {
						return fmt.Errorf("Word %v is in more than one index.", word)
					}
				}
			}
		}
	}

	return nil
}

func (ci *CombinedIndex) GetNumberOfItems() int {
	return ci.total_size
}

func (ci *CombinedIndex) GetVectorLength() int {
	return ci.vector_length
}

func (ci *CombinedIndex) WordToItemIndex(word string) ItemIndex {
	for _, item := range ci.indices {
		item_index := (*item.index).WordToItemIndex(word)

		if (&item_index).IsPresent() {
			return item_index + ItemIndex(item.offset)
		}
	}

	return -1
}

func (ci *CombinedIndex) find_vector_index_for_item_index(item_index ItemIndex) (ItemIndex, *Contextionary, error) {
	item := int(item_index)

	for _, idx := range ci.indices {
		if item >= idx.offset && item < (idx.offset+idx.size) {
			return ItemIndex(item - idx.offset), idx.index, nil
		}
	}

	return 0, nil, fmt.Errorf("out of index")
}

func (ci *CombinedIndex) ItemIndexToWord(item ItemIndex) (string, error) {
	offsetted_index, vi, err := ci.find_vector_index_for_item_index(item)

	if err != nil {
		return "", err
	}

	word, err := (*vi).ItemIndexToWord(offsetted_index)
	return word, err
}

func (ci *CombinedIndex) GetVectorForItemIndex(item ItemIndex) (*Vector, error) {
	offsetted_index, vi, err := ci.find_vector_index_for_item_index(item)

	if err != nil {
		return nil, err
	}

	word, err := (*vi).GetVectorForItemIndex(offsetted_index)
	return word, err
}

// Compute the distance between two items.
func (ci *CombinedIndex) GetDistance(a ItemIndex, b ItemIndex) (float32, error) {
	v1, err := ci.GetVectorForItemIndex(a)
	if err != nil {
		return 0.0, err
	}

	v2, err := ci.GetVectorForItemIndex(b)
	if err != nil {
		return 0.0, err
	}

	dist, err := v1.Distance(v2)
	if err != nil {
		return 0.0, err
	}

	return dist, nil
}

// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (ci *CombinedIndex) GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error) {
	vec, err := ci.GetVectorForItemIndex(item)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get vector for item index: %s", err)
	}

	return ci.GetNnsByVector(*vec, n, k)
}

type combined_nn_search_result struct {
	item ItemIndex
	dist float32
}

type combined_nn_search_results struct {
	items []combined_nn_search_result
	ci    *CombinedIndex
}

// SafeGetSimilarWords returns n similar words in the contextionary,
// examining k trees. It is guaratueed to have results, even if the word is
// not in the contextionary. In this case the list only contains the word
// itself. It can then still be used for exact match or levensthein-based
// searches against db backends.
func (ci *CombinedIndex) SafeGetSimilarWords(word string, n, k int) ([]string, []float32) {
	return safeGetSimilarWordsFromAny(ci, word, n, k)
}

// SafeGetSimilarWordsWithCertainty returns  similar words in the
// contextionary, if they are close enough to match the required certainty.
// It is guaratueed to have results, even if the word is not in the
// contextionary. In this case the list only contains the word itself. It can
// then still be used for exact match or levensthein-based searches against
// db backends.
func (ci *CombinedIndex) SafeGetSimilarWordsWithCertainty(word string, certainty float32) []string {
	return safeGetSimilarWordsWithCertaintyFromAny(ci, word, certainty)
}

func (a combined_nn_search_results) Len() int      { return len(a.items) }
func (a combined_nn_search_results) Swap(i, j int) { a.items[i], a.items[j] = a.items[j], a.items[i] }
func (a combined_nn_search_results) Less(i, j int) bool {
	// Sort on distance first, if those are the same, sort on lexographical order of the words.
	if a.items[i].dist == a.items[j].dist {
		wi, err := a.ci.ItemIndexToWord(a.items[i].item)
		if err != nil {
			panic("should be there")
		}

		wj, err := a.ci.ItemIndexToWord(a.items[j].item)
		if err != nil {
			panic("should be there")
		}
		return wi < wj
	} else {
		return a.items[i].dist < a.items[j].dist
	}
}

// Remove a certain element from the result search.
func (a *combined_nn_search_results) Remove(i int) {
	a.items = append(a.items[:i], a.items[i+1:]...)
}

// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (ci *CombinedIndex) GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error) {
	results := combined_nn_search_results{
		items: make([]combined_nn_search_result, 0),
		ci:    ci,
	}

	for _, item := range ci.indices {
		indices, floats, err := (*item.index).GetNnsByVector(vector, n, k)
		if err != nil {
			return nil, nil, err
		} else {
			for i, item_idx := range indices {
				results.items = append(results.items, combined_nn_search_result{item: item_idx + ItemIndex(item.offset), dist: floats[i]})
			}
		}
	}

	sort.Sort(results)

	// Now remove duplicates.
	for i := 1; i < len(results.items); {
		if results.items[i].item == results.items[i-1].item {
			results.Remove(i)
		} else {
			i++ // only increment if we're not removing.
		}
	}

	items := make([]ItemIndex, 0)
	floats := make([]float32, 0)

	var max_index int

	if n < len(results.items) {
		max_index = n
	} else {
		max_index = len(results.items)
	}

	for i := 0; i < max_index; i++ {
		items = append(items, results.items[i].item)
		floats = append(floats, results.items[i].dist)
	}

	return items, floats, nil
}
