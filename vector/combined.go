package vector

import (
	"fmt"
  "sort"
)

// Combine multiple indices, present them as one.
// It assumes that each index stores unique words
func CombineVectorIndices(indices []VectorIndex) (*CombinedIndex, error) {
	// We join the ItemIndex spaces the indivual indices, by
	// offsetting the 2nd ItemIndex with len(indices[0]),
	// the 3rd ItemIndex space with len(indices[0]) + len(indices[1]), etc.

	if len(indices) < 2 {
		return nil, fmt.Errorf("Less than two vector indices provided!")
	}

	offsets := make([]int, len(indices))
	sizes := make([]int, len(indices))
	var offset int = 0

	first_length := indices[0].GetVectorLength()

	for i := 0; i < len(indices); i++ {
		offsets[i] = offset
    sizes[i] = indices[i].GetNumberOfItems()
		offset += sizes[i]
		my_length := indices[1].GetVectorLength()

		if first_length != my_length {
			return nil, fmt.Errorf("vector length not equal")
		}
	}

  return &CombinedIndex { indices: indices, offsets: offsets, sizes: sizes, size: offset}, nil
}

type CombinedIndex struct {
	indices []VectorIndex
	offsets []int
	sizes   []int
  size    int
}

func (ci *CombinedIndex) GetNumberOfItems() int {
	var sum int = 0
  for _, vi := range ci.indices {
		sum += vi.GetNumberOfItems()
	}

	return sum
}

func (ci *CombinedIndex) GetVectorLength() int {
	return ci.indices[0].GetVectorLength()
}

func (ci *CombinedIndex) WordToItemIndex(word string) (ItemIndex) {
	for i, index := range ci.indices {
		item_index := index.WordToItemIndex(word)

		if (&item_index).IsPresent() {
			return item_index + ItemIndex(ci.offsets[i])
		}
	}

	return -1
}

func (ci *CombinedIndex) find_vector_index_for_item_index(item ItemIndex) (ItemIndex, *VectorIndex, error) {
  for i := 0; i < len(ci.indices); i++ {
    if ci.offsets[i] <= int(item) && int(item) < (ci.offsets[i] + ci.sizes[i]) {
      return (item - ItemIndex(ci.offsets[i])), &ci.indices[i], nil
    }
  }

  return 0, nil, fmt.Errorf("out of index")
}

func (ci *CombinedIndex) ItemIndexToWord(item ItemIndex) (string, error) {
  offset, vi, err := ci.find_vector_index_for_item_index(item)

  if err != nil {
    return "", err
  }

  offsetted_index := item - offset

  word, err := (*vi).ItemIndexToWord(offsetted_index)
  return word, err
}

func (ci *CombinedIndex) GetVectorForItemIndex(item ItemIndex) (*Vector, error) {
  offset, vi, err := ci.find_vector_index_for_item_index(item)

  if err != nil {
    return nil, err
  }

  offsetted_index := item - offset

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
    return 0, err
  }

  return dist, nil
}

// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (ci *CombinedIndex) GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error) {
  vec, err := ci.GetVectorForItemIndex(item)
  if err != nil {
    return nil, nil, err
  }

  return ci.GetNnsByVector(*vec, n, k)
}

type combined_nn_search_result struct {
  item ItemIndex
  dist float32
}

type combined_nn_search_results []combined_nn_search_result
func (a combined_nn_search_results) Len() int { return len(a) }
func (a combined_nn_search_results) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a combined_nn_search_results) Less(i, j int) bool { return a[i].dist < a[j].dist }


// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (ci *CombinedIndex) GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error) {
  results := make(combined_nn_search_results, 0)

  for _, vi := range(ci.indices) {
    indices, floats, err := vi.GetNnsByVector(vector, n, k)

    if err != nil {
      return nil, nil, err
    } else {
      for i, item := range(indices) {
        results = append(results, combined_nn_search_result { item: item, dist: floats[i] })
      }
    }
  }

  sort.Sort(results)

  items := make([]ItemIndex, 0)
  floats := make([]float32, 0)

  var max_index int

  if k < len(results) {
    max_index = k
  } else {
    max_index = len(results)
  }

  for i := 0; i < max_index; i ++ {
    items  = append(items, results[i].item)
    floats = append(floats, results[i].dist)
  }

  return items, floats, nil
}
