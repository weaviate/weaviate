package vector

import (
  "testing"
  "fmt"
  "log"
)

func TestLoadingIndex(*testing.T) {
  v, err := LoadVectorFromDisk("generator/glove.knn", "generator/glove.idx")

  if err != nil {
    log.Fatalf("Failed to load vector %+v", err)
  }

  print("nr words ", (*v).GetNumberOfItems(), "\n")
  print("vec width ", (*v).GetVectorLength(), "\n")

  for i := 0; i < (*v).GetNumberOfItems(); i ++ {
    s, _ := (*v).ItemIndexToWord(ItemIndex(i))
    fmt.Printf("%v %v\n", i, s)
  }

}
