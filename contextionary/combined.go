package contextionary

import (
	"fmt"
  "sort"
)

type CombinedIndex struct {
  indices []combinedIndex
  total_size    int
  vector_length int
}

type combinedIndex struct {
  offset int
  size int
  index *VectorIndex
}

// Combine multiple indices, present them as one.
// It assumes that each index stores unique words
func CombineVectorIndices(indices []VectorIndex) (*CombinedIndex, error) {
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
    size  := indices[i].GetNumberOfItems()

    combined_indices[i] = combinedIndex {
      offset: offset,
      size: size,
      index: &indices[i],
    }

		offset += size

		my_length := indices[i].GetVectorLength()

		if my_length != vector_length {
			return nil, fmt.Errorf("vector length not equal")
		}
	}

  return &CombinedIndex { indices: combined_indices, total_size: offset, vector_length: vector_length}, nil
}

func (ci *CombinedIndex) GetNumberOfItems() int {
	return ci.total_size
}

func (ci *CombinedIndex) GetVectorLength() int {
	return ci.vector_length
}

func (ci *CombinedIndex) WordToItemIndex(word string) (ItemIndex) {
  for _, item := range ci.indices {
		item_index := (*item.index).WordToItemIndex(word)

		if (&item_index).IsPresent() {
			return item_index + ItemIndex(item.offset)
		}
  }

	return -1
}

func (ci *CombinedIndex) find_vector_index_for_item_index(item_index ItemIndex) (ItemIndex, *VectorIndex, error) {
  item := int(item_index)

  for _, idx := range ci.indices {
    if item >= idx.offset && item < (idx.offset + idx.size) {
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

// Remove a certain element from the result search.
func (a *combined_nn_search_results) Remove(i int) {
  s := *a
  s = append(s[:i], s[i+1:]...)
  *a = s
}


// Get the n nearest neighbours of item, examining k trees.
// Returns an array of indices, and of distances between item and the n-nearest neighbors.
func (ci *CombinedIndex) GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error) {
  results := make(combined_nn_search_results, 0)

  for _, item := range(ci.indices) {
    indices, floats, err := (*item.index).GetNnsByVector(vector, n, k)

    if err != nil {
      return nil, nil, err
    } else {
      for i, item := range(indices) {
        results = append(results, combined_nn_search_result { item: item, dist: floats[i] })
      }
    }
  }

  sort.Sort(results)


  // Now remove duplicates.
  for i := 1; i < len(results); {
    if results[i].item == results[i-1].item {
      w, _:= ci.ItemIndexToWord(results[i].item)
      fmt.Printf("Removing %v\n", w)
      results.Remove(i)
    } else {
      w, _ := ci.ItemIndexToWord(results[i].item)
      fmt.Printf("Keeping %v\n", w)
      i++ // only increment if we're not removing.
    }
  }
  fmt.Printf("DONE\n")

  items := make([]ItemIndex, 0)
  floats := make([]float32, 0)

  var max_index int

  if n < len(results) {
    max_index = n
  } else {
    max_index = len(results)
  }

  for i := 0; i < max_index; i ++ {
    items  = append(items, results[i].item)
    floats = append(floats, results[i].dist)
  }

  return items, floats, nil
}
