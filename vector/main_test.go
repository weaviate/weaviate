package vector

import (
	"testing"
  "io/ioutil"
  "os"
  "fmt"
  "math"

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


func TestMMappedIndex(t *testing.T) {
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

      // and check that it's correct
      areEqual, err := vector.EqualFloats(vt.vec)
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

        wi_a := (*vi).WordToItemIndex(vt_a.word)
        wi_b := (*vi).WordToItemIndex(vt_b.word)

        annoy_dist, err := (*vi).GetDistance(wi_a, wi_b)
        if err != nil {
          t.Errorf("Could not compute distance")
        }
        simple_dist := dist(vt_a.vec, vt_b.vec)

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
    var apple_pie = NewVector(/* centroid of apple and pie */ []float32{0.6, 0.5,0})

    fruit_idx :=  (*vi).WordToItemIndex("fruit")
    apple_idx :=  (*vi).WordToItemIndex("apple")
    pie_idx :=  (*vi).WordToItemIndex("pie")

    res, distances, err := (*vi).GetNnsByVector(apple_pie, 3, 3)
    if err != nil {
      t.Errorf("GetNNs failed!")
    }
    if len(res) != 3 {
      t.Errorf("Wrong number of items returned")
    }

    if res[0] != fruit_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[1])
      t.Errorf("apple pie should be closest to fruit !, but was '%v'", closest_to)
    }

    if res[1] != apple_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[1])
      t.Errorf("apple pie should be closest to apple!, but was '%v'", closest_to)
    }

    if res[2] != pie_idx {
      closest_to, _ := (*vi).ItemIndexToWord(res[2])
      t.Errorf("apple pie should be 2nd closest to pie!, but was '%v'", closest_to)
    }

//    apple_fruit_dist := sum_dist(vectorTests[0].vec, vectorTests[3].vec)
    v1 := NewVector(vectorTests[0].vec)
    v2 := NewVector(vectorTests[3].vec)
    apple_fruit_dist := v1.Distance(&v2)
    if !equal_float_epsilon(distances[0], apple_fruit_dist, 0.00001) {
      t.Errorf("Wrong distance for fruit, expect %v, got %v", apple_fruit_dist, distances[0])
    }

    if !equal_float_epsilon(distances[1], 0, 0) {
      t.Errorf("Wrong distance for apple, got %v", distances[1])
    }

    if !equal_float_epsilon(distances[2], 0, 0) {
      t.Errorf("Wrong distance for pie, got %v", distances[2])
    }
  });
}

func dist(a []float32, b []float32) float32 {
  var sum float32

  for i := 0; i < len(a); i ++ {
    x := a[i] - b[i]
    sum += x*x
  }

  return float32(math.Sqrt(float64(sum)))
}

func sum_dist(a []float32, b []float32) float32 {
  var sum float32

  for i := 0; i < len(a); i ++ {
    x := a[i] - b[i]
    sum += float32(math.Abs(float64(x)))
  }

  return float32(math.Sqrt(float64(sum)))
}


func equal_float_epsilon(a float32, b float32, epsilon float32) bool {
  var min, max float32

  if a < b {
    min = a
    max = b
  } else {
    min = b
    max =a
  }

  return max < (min + epsilon)
}
