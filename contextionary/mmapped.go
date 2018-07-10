package contextionary

import (
	"fmt"
	annoy "github.com/creativesoftwarefdn/weaviate/contextionary/annoyindex"
)

type mmappedIndex struct {
	word_index *Wordlist
	knn        annoy.AnnoyIndex
}

func (m *mmappedIndex) GetNumberOfItems() int {
	return int(m.word_index.numberOfWords)
}

// Returns the length of the used vectors.
func (m *mmappedIndex) GetVectorLength() int {
	return int(m.word_index.vectorWidth)
}

func (m *mmappedIndex) WordToItemIndex(word string) (ItemIndex) {
  return m.word_index.FindIndexByWord(word)
}

func (m *mmappedIndex) ItemIndexToWord(item ItemIndex) (string, error) {
  if item >= 0 && item <= m.word_index.GetNumberOfWords() {
    return m.word_index.getWord(item), nil
  } else {
    return "", fmt.Errorf("Index out of bounds")
  }
}

func (m *mmappedIndex) GetVectorForItemIndex(item ItemIndex) (*Vector, error) {
 if item >= 0 && item <= m.word_index.GetNumberOfWords() {
    var floats []float32
    m.knn.GetItem(int(item), &floats)

    return &Vector{ floats }, nil
  } else {
    return nil, fmt.Errorf("Index out of bounds")
  }
}

// Compute the distance between two items.
func (m *mmappedIndex) GetDistance(a ItemIndex, b ItemIndex) (float32, error) {
 if a >= 0 && b >=0 && a <= m.word_index.GetNumberOfWords() && b <= m.word_index.GetNumberOfWords() {
    return m.knn.GetDistance(int(a),int(b)), nil
  } else {
    return 0, fmt.Errorf("Index out of bounds")
  }
}

func (m *mmappedIndex) GetNnsByItem(item ItemIndex, n int, k int) ([]ItemIndex, []float32, error) {
  if item >= 0 && item <= m.word_index.GetNumberOfWords() {
    var items []int
    var distances []float32

    m.knn.GetNnsByItem(int(item), n, k, &items, &distances)

    var indices []ItemIndex = make([]ItemIndex, len(items))
    for i, x := range(items) {
      indices[i] = ItemIndex(x)
    }

    return indices, distances, nil
  } else {
    return nil, nil, fmt.Errorf("Index out of bounds")
  }
}

func (m *mmappedIndex) GetNnsByVector(vector Vector, n int, k int) ([]ItemIndex, []float32, error) {
  if len(vector.vector) == m.GetVectorLength() {
    var items []int
    var distances []float32

    m.knn.GetNnsByVector(vector.vector, n, k, &items, &distances)

    var indices []ItemIndex = make([]ItemIndex, len(items))
    for i, x := range(items) {
      indices[i] = ItemIndex(x)
    }

    return indices, distances, nil
  } else {
    return nil, nil, fmt.Errorf("Wrong vector length provided")
  }
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
