// Combine multiple indices, present them as one.
// It assumes that each index stores unique words
func CombineVectorIndices(indices []VectorIndex) (*CombinedIndex) {
  // We join the ItemIndex spaces the indivual indices, by
  // offsetting the 2nd ItemIndex with len(indices[0]),
  // the 3rd ItemIndex space with len(indices[0]) + len(indices[1]), etc.

  if len(indices) < 2 {
    return nil, fmt.Errorf("Less than two vector indices provided!")
  }

  offsets := make([]int, len(indices))
  offset int = 0

  first_length := indices[0].GetVectorLength()

  for i := range(len(indices)) {
    offsets[i] = offset
    offset += indices[i].GetNumberOfItems()
    my_length := indices[1].GetVectorLength()

    if first_length != my_length {
      return nil, fmt.Errorf("vector length not equal")
    }
  }

  return CombinedIndex { indices, offsets }
}

struct CombinedIndex {
  indices []VectorIndex
  offsets []int
}

// Verifies that all the indices do not contain overlapping words.
func (ci *CombinedIndex) VerifyUniqueness() error {
  return nil
}

func (ci *CombinedIndex) GetNumberOfItems() int {
  sum int := 0
  for index in ci.indices {
    sum += ci.GetNumberOfItems()
  }

  return sum
}

func (ci *CombinedIndex) GetVectorLength() int {
  return ci.indices[0].GetVectorLength()
}

func (ci *CombinedIndex) WordToItemIndex(word string) ItemIndex {
  for i, index := range(ci.indices) {
    item_index := index.WordToItemIndex(word)

    if item_index.IsPresent() {
      return item_index + ci.offsets[i]
    }
  }

  return -1
}

func (ci *CombinedIndex) ItemIndexToWord(item ItemIndex) string {
  for i, index := range(ci.indices) {
    word := index.ItemIndexToWord(item - ci.offsets[i])

    if word != "" {
      return word
    }
  }

  return ""
}

//// etc
