package vector

import (
  annoy "github.com/creativesoftwarefdn/weaviate/vector/annoyindex"
  word_index "github.com/creativesoftwarefdn/weaviate/vector/wordindex"
)

type vectorIndex struct {
  knn_index annoy.AnnoyIndex
  word_index wordIndex
}

type wordIndex struct {
  // mmaped stuff
}
