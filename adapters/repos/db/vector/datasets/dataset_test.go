//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build benchmark
// +build benchmark

package datasets

import (
	"math"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestLoadDataset(t *testing.T) {
	logger := logrus.New()
	hf := NewHubDataset("trengrj/ann-datasets", "dbpedia-openai-ada002-1536-angular-100k", logger)

	// Load training data
	t.Log("Loading training data...")
	ids, vectors, err := hf.LoadTrainData()
	if err != nil {
		t.Fatalf("Failed to load training data: %v", err)
	}

	// Assert training data structure
	if len(vectors) == 0 {
		t.Fatal("Expected non-zero number of training vectors")
	}
	if len(ids) != len(vectors) {
		t.Fatalf("Expected equal number of IDs (%d) and vectors (%d)", len(ids), len(vectors))
	}
	if len(vectors) < 100000 {
		t.Errorf("Expected at least 100,000 training vectors, got %d", len(vectors))
	}

	t.Logf("Loaded %d training vectors", len(vectors))

	// Assert first training vector structure and values
	if len(vectors) >= 1 {
		vector1 := vectors[0]
		if len(vector1) == 0 {
			t.Fatal("Expected non-zero vector dimensions")
		}

		// Check first 5 dimensions match expected values (with tolerance for floating point)
		expectedDims1 := []float32{-0.013902083, 0.016943572, 0.013771547, -0.0066899695, -0.026394377}
		if len(vector1) >= 5 {
			for i, expected := range expectedDims1 {
				if math.Abs(float64(vector1[i]-expected)) > 1e-6 {
					t.Errorf("Training vector 1 dimension %d: expected %f, got %f", i, expected, vector1[i])
				}
			}
			t.Logf("Training vector 1 (ID: %d) first 5 dimensions: %v", ids[0], vector1[:5])
		}
	}

	// Load test data
	t.Log("Loading test data...")
	neighbors, testVectors, err := hf.LoadTestData()
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	// Assert test data structure
	if len(testVectors) == 0 {
		t.Fatal("Expected non-zero number of test vectors")
	}
	if len(neighbors) != len(testVectors) {
		t.Fatalf("Expected equal number of neighbors (%d) and test vectors (%d)", len(neighbors), len(testVectors))
	}
	if len(testVectors) < 900 {
		t.Errorf("Expected at least 900 test vectors, got %d", len(testVectors))
	}

	t.Logf("Loaded %d test vectors and %d neighbor lists", len(testVectors), len(neighbors))

	// Assert first test vector structure and values
	if len(testVectors) >= 1 {
		testVector1 := testVectors[0]
		if len(testVector1) == 0 {
			t.Fatal("Expected non-zero test vector dimensions")
		}

		// Check first 5 dimensions match expected values
		expectedTestDims1 := []float32{-0.027109032, -0.019073945, 0.016458161, -0.023066457, -0.0012101129}
		if len(testVector1) >= 5 {
			for i, expected := range expectedTestDims1 {
				if math.Abs(float64(testVector1[i]-expected)) > 1e-6 {
					t.Errorf("Test vector 1 dimension %d: expected %f, got %f", i, expected, testVector1[i])
				}
			}
			t.Logf("Test vector 1 first 5 dimensions: %v", testVector1[:5])
		}

		// Assert neighbors structure
		if len(neighbors) > 0 {
			neighbors1 := neighbors[0]
			if len(neighbors1) == 0 {
				t.Fatal("Expected non-zero number of neighbors")
			}
			if len(neighbors1) < 100 {
				t.Errorf("Expected at least 100 neighbors, got %d", len(neighbors1))
			}

			// Check that neighbors are valid IDs (within training set range)
			for _, neighborID := range neighbors1 {
				if neighborID >= uint64(len(vectors)) {
					t.Errorf("Invalid neighbor ID: %d (should be 0-%d)", neighborID, len(vectors)-1)
				}
			}

			t.Logf("Test vector 1 neighbors count: %d", len(neighbors1))
		}
	}
}

func BenchmarkLoadData(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	hf := NewHubDataset("trengrj/ann-datasets", "dbpedia-openai-ada002-1536-angular-100k", logger)
	// Warmup runs to ensure that the data is downloaded.
	ids, vectors, _ := hf.LoadTrainData()
	n, d := len(ids), len(vectors[0])
	numFloats := n * d

	b.Run("Train", func(b *testing.B) {
		for b.Loop() {
			hf.LoadTrainData()
		}
		b.ReportMetric(float64(b.Elapsed().Milliseconds())/float64(b.N), "ms/op")
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*numFloats), "ns/float")
	})

	b.Run("Test", func(b *testing.B) {
		for b.Loop() {
			hf.LoadTestData()
		}
		b.ReportMetric(float64(b.Elapsed().Milliseconds())/float64(b.N), "ms/op")
	})
}
