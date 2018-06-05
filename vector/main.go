// Opaque type that models an index number used to identify a word.
type ItemIndex int

func (*ItemIndex i) IsPresent() {
  return i >= 0
}

// Opque type that models a fixed-length vector.
type Vector {
  vector []float32
}

// API to decouple the K-nn interface that is needed for Weaviate from a concrete implementation.
type VectorIndex interface {
  // Return the number of items that is stored in the0
  GetNumberOfItems() int

  // Returns the length of the used vectors.
  GetVectorLength() int

  // Look up a word, return an index.
  // Check for presence of the index with index.IsPresent()
  WordToItemIndex(word string) ItemIndex

  // Based on an index, return the assosiated word.
  ItemIndexToWord(ItemIndex) string

  // Get the vector of an item index.
  GetVectorForItemIndex(item ItemIndex) Vector

  // Compute the distance between two items.
  GetDistance(a ItemIndex, b ItemIndex) Vector

  // Get the n nearest neighbours of item, examining k trees.
  // Returns an array of indices, and of distances between item and the n-nearest neighbors.
  GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32)

  // Get the n nearest neighbours of item, examining k trees.
  // Returns an array of indices, and of distances between item and the n-nearest neighbors.
  GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32),
}
