package vector

// Opaque type that models an index number used to identify a word.
type ItemIndex int

func (i *ItemIndex) IsPresent() bool {
  return *i >= 0
}

// Opque type that models a fixed-length vector.
type Vector struct {
  vector []float32
}

func (v *Vector) Vector() (* []float32){
  return &v.vector
}

// API to decouple the K-nn interface that is needed for Weaviate from a concrete implementation.
type VectorIndex interface {
  // Return the number of items that is stored in the index.
  GetNumberOfItems() int

  // Returns the length of the used vectors.
  GetVectorLength() int

  // Look up a word, return an index.
  // Check for presence of the index with index.IsPresent()
  WordToItemIndex(word string) (ItemIndex, error)

  // Based on an index, return the assosiated word.
  ItemIndexToWord(item ItemIndex) (string, error)

  // Get the vector of an item index.
  GetVectorForItemIndex(item ItemIndex) (*Vector, error)

  // Compute the distance between two items.
  GetDistance(a ItemIndex, b ItemIndex) (*Vector, error)

  // Get the n nearest neighbours of item, examining k trees.
  // Returns an array of indices, and of distances between item and the n-nearest neighbors.
  GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error)

  // Get the n nearest neighbours of item, examining k trees.
  // Returns an array of indices, and of distances between item and the n-nearest neighbors.
  GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32)
}
