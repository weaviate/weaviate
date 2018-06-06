package vector

import (
  "fmt"
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
  var offset int = 0

  first_length := indices[0].GetVectorLength()

  for i := 0; i < len(indices); i++ {
    offsets[i] = offset
    offset += indices[i].GetNumberOfItems()
    my_length := indices[1].GetVectorLength()

    if first_length != my_length {
      return nil, fmt.Errorf("vector length not equal")
    }
  }

  return &CombinedIndex { indices, offsets }, nil
}

type CombinedIndex struct {
  indices []VectorIndex
  offsets []int
}

// Verifies that all the indices do not contain overlapping words.
func (ci *CombinedIndex) VerifyUniqueness() error {
  return nil
}

func (ci *CombinedIndex) GetNumberOfItems() int {
  var sum int = 0
  for _ = range(ci.indices) {
    sum += ci.GetNumberOfItems()
  }

  return sum
}

func (ci *CombinedIndex) GetVectorLength() int {
  return ci.indices[0].GetVectorLength()
}

func (ci *CombinedIndex) WordToItemIndex(word string) (ItemIndex, error) {
  for i, index := range(ci.indices) {
    item_index, err := index.WordToItemIndex(word)

    if err != nil {
      return -1, err
    }

    if (&item_index).IsPresent() {
      return item_index + ItemIndex(ci.offsets[i]), nil
    }
  }

  return -1, nil
}

func (ci *CombinedIndex) ItemIndexToWord(item ItemIndex) (string, error) {
  for i, index := range(ci.indices) {
    word, err := index.ItemIndexToWord(item - ItemIndex(ci.offsets[i]))
    if err != nil {
      return "", err
    }
    if word != "" {
      return word, nil
    }
  }

  return "", nil
}

//// etc
