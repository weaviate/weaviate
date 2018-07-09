package vector

import (
	annoy "github.com/creativesoftwarefdn/weaviate/vector/annoyindex"
  "sort"
)

type MemoryIndex struct {
  dimensions int
  words []string
	knn   annoy.AnnoyIndex
}


// The rest of this file concerns itself with building the Memory Index.
// This is done from the MemoryIndexBuilder struct.

type MemoryIndexBuilder struct {
  dimensions int
  trees int
  word_vectors mib_pairs
}

type mib_pair struct {
  word   string
  vector Vector
}

// Define custom type, and implement functions required for sort.Sort.
type mib_pairs []mib_pair

func (a mib_pairs) Len() int { return len(a) }
func (a mib_pairs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a mib_pairs) Less(i, j int) bool { return a[i].word < a[j].word }

// Construct a new builder.
func InMemoryBuilder(dimensions int, trees int) *MemoryIndexBuilder {
  mib := MemoryIndexBuilder {
    dimensions: dimensions,
    trees: trees,
    word_vectors: make([]mib_pair, 0),
  }

  return &mib
}

// Add a word and it's vector to the builder.
func (mib *MemoryIndexBuilder) AddWord(word string, vector Vector) {
  wv := mib_pair { word: word, vector: vector }
  mib.word_vectors = append(mib.word_vectors, wv)
}

// Build an efficient lookup iddex from the builder.
func (mib *MemoryIndexBuilder) Build() *MemoryIndex {
  mi := MemoryIndex {
    words: make([]string, 0),
    knn: annoy.NewAnnoyIndexEuclidean(mib.dimensions),
  }

  // First sort the words; this way we can do binary search on the words.
  sort.Sort(mib.word_vectors)

  // Then fill up the data in the MemoryIndex
  for i, pair := range(mib.word_vectors) {
    mi.words = append(mi.words, pair.word)
    mi.knn.AddItem(i, pair.vector.vector)
  }

  // And instruct Annoy to build it's index
  mi.knn.Build(mib.trees)

  return &mi
}
