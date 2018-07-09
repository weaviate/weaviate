package vector

import (
	"testing"
  "io/ioutil"
  "os"
  "fmt"

  "github.com/creativesoftwarefdn/weaviate/vector/generator"
)

// Test data
// Changing this data might invalidate the nearest neighbours test!
var vectorTests = []struct {
  word string
  vec []float32
}{
  { "apple",    []float32{1,   0, 0} },
  { "pie",      []float32{0,   1, 0} },
  { "computer", []float32{0,   0, 1} },
  { "fruit",    []float32{0.8, 0, 0} },
  { "company",  []float32{0,   0, 2} },
}

func _TestMMappedIndex(t *testing.T) {
  tempdir, err := ioutil.TempDir("", "weaviate-vector-test")

  if err != nil {
    t.Errorf("Could not create temporary directory, %v", err)
  }

  defer os.RemoveAll(tempdir)

  // First generate the csv input fileformat based on the test data.
  var dataset = ""

  for i := 0; i < len(vectorTests); i++ {
    vt := vectorTests[i]
    dataset += vt.word + " "
    for j := 0; j < len(vt.vec) - 1; j++ {
      dataset += fmt.Sprintf("%f ", vt.vec[j])
    }
    dataset += fmt.Sprintf("%f\n", vt.vec[len(vt.vec)-1])
  }

  err = ioutil.WriteFile(tempdir + "/glove.txt", []byte(dataset), 0644)
  if err != nil {
    t.Errorf("Could not create input file: %v", err)
  }

  t.Run("Generating index", func(t *testing.T) {
    // Now build an index based on this
    var gen_opts generator.Options
    gen_opts.VectorCSVPath = tempdir + "/glove.txt"
    gen_opts.TempDBPath    = tempdir + "/tempdb"
    gen_opts.OutputPrefix  = tempdir + "/glove"
    gen_opts.K = 3
    generator.Generate(gen_opts)
  })

  // And load the index.
  vi, err := LoadVectorFromDisk(tempdir + "/glove.knn", tempdir + "/glove.idx")
  if err != nil {
    t.Errorf("Could not load vectors from disk: %v", err)
  }

  shared_tests(t, vi)
}

func _TestInMemoryIndex(t *testing.T) {
  builder := InMemoryBuilder(3)
  for i := 0; i < len(vectorTests); i ++ {
    v := vectorTests[i]
    builder.AddWord(v.word, NewVector(v.vec))
  }

  memory_index := VectorIndex(builder.Build(3))

  shared_tests(t, &memory_index)
}

func TestCombinedIndex(t *testing.T) {
  builder1 := InMemoryBuilder(3)
  builder2 := InMemoryBuilder(3)

  split := 3

  for i := 0; i < split; i ++ {
    v := vectorTests[i]
    builder1.AddWord(v.word, NewVector(v.vec))
  }

  for i := split; i < len(vectorTests); i ++ {
    v := vectorTests[i]
    builder2.AddWord(v.word, NewVector(v.vec))
  }

  memory_index1 := VectorIndex(builder1.Build(3))
  memory_index2 := VectorIndex(builder2.Build(3))

  var indices []VectorIndex = []VectorIndex { memory_index1, memory_index2, }

  combined_index, err := CombineVectorIndices(indices)
  if err != nil {
    t.Errorf("Combining failed")
    t.FailNow()
  }

  vi := VectorIndex(combined_index)

  shared_tests(t, &vi)
}

func shared_tests(t *testing.T, vi *VectorIndex) {
  t.Run("Number of elements is correct", func (t *testing.T) {
    expected := 5
    found := (*vi).GetNumberOfItems()
    if found != expected {
      t.Errorf("Expected to have %v items, but found %v", expected, found)
    }
  })

  t.Run("Iterate over all items", func (t *testing.T) {
    // Iterate over all items. Check index -> word, and lookup word -> index
    length := ItemIndex((*vi).GetNumberOfItems())
    for i := ItemIndex(0); i < length; i++ {
      word, err := (*vi).ItemIndexToWord(ItemIndex(i))
      if err != nil {
        t.Errorf("Could not get item of index %+v, because: %+v", i, err)
      }

      i2 := (*vi).WordToItemIndex(word)

      if i2 != i {
        t.Errorf("Index -> Word -> Index failed!. i=%v, w=%v i2=%v", i, word, i2)
      }
    }
  })

  t.Run("Check that feature vectors are stored properly", func (t *testing.T) {
    for i := 0; i < len(vectorTests); i++ {
      vt := vectorTests[i]
      word_index := (*vi).WordToItemIndex(vt.word)
      if !word_index.IsPresent() {
        t.Errorf("Could not find word %v", vt.word)
      }
      // Get back the feature vectors.
      vector, err := (*vi).GetVectorForItemIndex(word_index)
      if err != nil {
        t.Errorf("Could not get vector")
      }

      if vector == nil {
        t.Errorf("Vector missing!")
        t.FailNow()
      }

      // and check that it's correct
      vtvec := NewVector(vt.vec)
      areEqual, err := vector.Equal(&vtvec)
      if err != nil {
        t.Errorf("Could not compare the two vectors: %v", err)
      }

      if !areEqual {
        t.Errorf("Feature vector %v incorrect (word: %v). Expected %v, got %v", i, vt.word, vt.vec, vector.vector)
      }
    }
  })

  t.Run("Test that the distances between all pairs of test data is correct", func (t *testing.T) {
    for i := 0; i < len(vectorTests); i ++ {
      for j := 0; j < len(vectorTests); j ++ {
        vt_a := vectorTests[i]
        vt_b := vectorTests[j]
        vt_a_vec := NewVector(vt_a.vec)
        vt_b_vec := NewVector(vt_b.vec)

        wi_a := (*vi).WordToItemIndex(vt_a.word)
        wi_b := (*vi).WordToItemIndex(vt_b.word)

        annoy_dist, err := (*vi).GetDistance(wi_a, wi_b)
        if err != nil {
          t.Errorf("Could not compute distance")
        }

        simple_dist, err := vt_a_vec.Distance(&vt_b_vec)
        if err != nil { panic("should be same length") }

        if !equal_float_epsilon(annoy_dist, simple_dist, 0.00003) {
          t.Errorf("Distance between %v and %v incorrect; %v (annoy) vs %v (test impl)", vt_a.word, vt_b.word, annoy_dist, simple_dist)
        }
      }
    }
  })

  t.Run("Test nearest neighbours apple & fruit", func (t *testing.T) {
    apple_idx :=  (*vi).WordToItemIndex("apple")
    fruit_idx :=  (*vi).WordToItemIndex("fruit")

    res, distances, err := (*vi).GetNnsByItem(fruit_idx, 2, 3)
    if err != nil {
      t.Errorf("GetNNs failed!")
    }
    if len(res) != 2 {
      t.Errorf("Wrong number of items returned")
      t.FailNow()
    }
    // res[0] will be fruit itself.
    if res[1] != apple_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[0])
      t.Errorf("Fruit should be closest to apple, but was '%v'", closest_to)
    }
    if !equal_float_epsilon(distances[1], 0.2, 0.0002) {
      t.Errorf("Wrong distances!, got %v", distances[1])
    }
  })

  t.Run("Test nearest neighbours computer & company", func (t *testing.T) {
    company_idx :=  (*vi).WordToItemIndex("company")
    computer_idx :=  (*vi).WordToItemIndex("computer")

    res, distances, err := (*vi).GetNnsByItem(company_idx, 2, 3)
    if err != nil {
      t.Errorf("GetNNs failed!")
    }
    if len(res) != 2 {
      t.Errorf("Wrong number of items returned")
      t.FailNow()
    }
    // res[0] will be company itself.
    if res[1] != computer_idx {
      t.Errorf("computer should be closest to company!")
    }

    if !equal_float_epsilon(distances[1], 1, 0.0002) {
      t.Errorf("Wrong distances!, got %v", distances[1])
    }
  })

  t.Run("Test k-nearest from vector", func (t *testing.T) {
    var apple_pie = NewVector(/* centroid of apple and pie */ []float32{0.5, 0.5,0})

    fruit_idx :=  (*vi).WordToItemIndex("fruit")
    apple_idx :=  (*vi).WordToItemIndex("apple")
    pie_idx :=  (*vi).WordToItemIndex("pie")

    res, distances, err := (*vi).GetNnsByVector(apple_pie, 3, 3)
    if err != nil {
      t.Errorf("GetNNs failed: %v", err)
      t.FailNow()
    }
    if len(res) != 3 {
      t.Errorf("Wrong number of items returned")
      t.FailNow()
    }

    if res[0] != fruit_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[1])
      t.Errorf("fruit should be closest to fruit !, but was '%v'", closest_to)
    }

    if res[1] != apple_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[1])
      t.Errorf("apple should be 2nd closest to apple!, but was '%v'", closest_to)
    }

    if res[2] != pie_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[2])
      t.Errorf("pie should be 3rd closest to pie!, but was '%v'", closest_to)
    }

    v_fruit, err := (*vi).GetVectorForItemIndex(fruit_idx);
    if err != nil { t.Errorf("could not fetch fruit vector"); return }

    v_apple, err := (*vi).GetVectorForItemIndex(apple_idx);
    if err != nil { panic("could not fetch apple vector") }

    v_pie, err := (*vi).GetVectorForItemIndex(pie_idx);
    if err != nil { panic("could not fetch pie vector") }

    distance_to_fruit, err := apple_pie.Distance(v_fruit)
    if err != nil { panic("should be same length") }
    if !equal_float_epsilon(distances[0], distance_to_fruit, 0.0001) {
      t.Errorf("Wrong distance for fruit, expect %v, got %v", distance_to_fruit, distances[0])
    }

    distance_to_apple, err := apple_pie.Distance(v_apple)
    if err != nil { panic("should be same length") }
    if !equal_float_epsilon(distances[1], distance_to_apple, 0.0001) {
      t.Errorf("Wrong distance for apple, got %v", distances[1])
    }

    distance_to_pie, err := apple_pie.Distance(v_pie)
    if err != nil { panic("should be same size") }
    if !equal_float_epsilon(distances[2], distance_to_pie, 0.0001) {
      t.Errorf("Wrong distance for pie, expected %v, got %v", distance_to_pie, distances[2])
    }
  });
}

func equal_float_epsilon(a float32, b float32, epsilon float32) bool {
  var min, max float32

  if a < b {
    min = a
    max = b
  } else {
    min = b
    max = a
  }

  return max <= (min + epsilon)
}
