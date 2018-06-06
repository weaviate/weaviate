package vector

import (
  annoy "github.com/creativesoftwarefdn/weaviate/vector/annoyindex"
  "fmt"
)

type mmappedIndex struct {
    word_index *Wordlist
    knn  annoy.AnnoyIndex
}

func (m *mmappedIndex) GetNumberOfItems() int {
  return int(m.word_index.numberOfWords)
}

  // Returns the length of the used vectors.
func (m *mmappedIndex)   GetVectorLength() int {
  return int(m.word_index.vectorWidth)
}

func (m *mmappedIndex)   WordToItemIndex(word string) (ItemIndex, error) {
  return 0, nil
}

func (m *mmappedIndex)   ItemIndexToWord(item ItemIndex) (string, error) {
  return m.word_index.getWord(item), nil
}

func (m *mmappedIndex)   GetVectorForItemIndex(item ItemIndex) (*Vector, error) {
  return nil, nil
}

  // Compute the distance between two items.
func (m *mmappedIndex)   GetDistance(a ItemIndex, b ItemIndex) (*Vector, error) {
  return nil, nil
}

func (m *mmappedIndex)   GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error) {
  return make([]ItemIndex, 0), make([]float32, 0), nil
}

func (m *mmappedIndex)   GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32) {
  return make([]ItemIndex, 0), make([]float32, 0)
}

func LoadVectorFromDisk(annoy_index string, word_index_file_name string) (*VectorIndex, error) {
  word_index, err := LoadWordlist(word_index_file_name)

  if err != nil {
    return nil, fmt.Errorf("Could not load vector: %+v", err)
  }

  knn := annoy.NewAnnoyIndexEuclidean(int(word_index.vectorWidth))
  knn.Load(annoy_index)

  var idx *mmappedIndex = new(mmappedIndex)
  idx.word_index = word_index
  idx.knn = knn

  var blah VectorIndex = VectorIndex(idx)
  return &blah, nil
}
