type interface WordList {
  NumberOfWords() int
  LookupWordByItemIndex(item ItemIndex) string
  LookupItemByWord(string) ItemIndex
}

type interface WordListBuilder {
  Add(word string)
  Finalize() WordList
}
