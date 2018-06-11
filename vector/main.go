package vector

import (
  "fmt"
//  "math"
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
    return v.EqualFloats(other.vector)
}

func (v *Vector) EqualFloats(floats []float32) (bool, error) {
  if len(v.vector) != len(floats) {
    return false, fmt.Errorf("Vectors have different dimensions")
  }

  for i, v := range v.vector {
    if floats[i] != v {
      return false, nil
    }
  }

  return true, nil
}

func dot(a []float32, b []float32) float32 {
  var sum float32
  if len(a) != len(b) {
    panic("should be called with equal length slices")
  }

  for i := 0; i < len(a); i++ {
    sum += a[i] * b[i]
  }

  return sum
}

func (v *Vector) Distance(other *Vector) float32 {
  a := v.vector
  b := other.vector

  return dot(a,b) / (dot(a,a) * dot(b,b))
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
