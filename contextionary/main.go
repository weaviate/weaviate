package contextionary

import (
  "fmt"
  "math"
)

// Opaque type that models an index number used to identify a word.
type ItemIndex int

func (i *ItemIndex) IsPresent() bool {
	return *i >= 0
}

// Opque type that models a fixed-length vector.
type Vector struct {
	vector []float32
}

func NewVector(vector []float32) Vector {
  return Vector { vector }
}

func (v *Vector) Equal(other *Vector) (bool, error) {
  if len(v.vector) != len(other.vector) {
    return false, fmt.Errorf("Vectors have different dimensions")
  }

  for i, v := range v.vector {
    if other.vector[i] != v {
      return false, nil
    }
  }

  return true, nil
}

func (v *Vector) Len() int {
  return len(v.vector)
}


func (v *Vector) ToString() string {
  str := "["
  first := true
  for _, i := range v.vector {
    if first {
      first = false
    } else {
      str += ", "
    }

    str += fmt.Sprintf("%.3f", i)
  }

  str += "]"

  return str
}

func (v *Vector) Distance(other *Vector) (float32, error) {
  var sum float32

  if len(v.vector) != len(other.vector) {
    return 0.0, fmt.Errorf("Vectors have different dimensions")
  }

  for i := 0; i < len(v.vector); i ++ {
    x := v.vector[i] - other.vector[i]
    sum += x*x
  }

  return float32(math.Sqrt(float64(sum))), nil
}

// API to decouple the K-nn interface that is needed for Weaviate from a concrete implementation.
type VectorIndex interface {
	// Return the number of items that is stored in the index.
	GetNumberOfItems() int

	// Returns the length of the used vectors.
	GetVectorLength() int

	// Look up a word, return an index.
	// Check for presence of the index with index.IsPresent()
	WordToItemIndex(word string) (ItemIndex)

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
}
