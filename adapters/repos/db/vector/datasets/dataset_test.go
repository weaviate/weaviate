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

package datasets

// func TestLoadDataset(t *testing.T) {
// 	logger := logrus.New()
// 	hf := NewHubDataset("trengrj/ann-datasets", "dbpedia-openai-100k", logger)

// 	// Load training data
// 	t.Log("Loading training data...")
// 	ids, vectors, err := hf.LoadTrainingData()
// 	if err != nil {
// 		t.Fatalf("Failed to load training data: %v", err)
// 	}

// 	t.Logf("Loaded %d training vectors", len(vectors))

// 	// Print first 5 dimensions of first and second training vectors
// 	if len(vectors) >= 2 {
// 		t.Logf("Training vector 1 (ID: %d) first 5 dimensions: %v", ids[0], vectors[0][:5])
// 		t.Logf("Training vector 2 (ID: %d) first 5 dimensions: %v", ids[1], vectors[1][:5])
// 	}

// 	// Load test data
// 	t.Log("Loading test data...")
// 	neighbors, testVectors, err := hf.LoadTestData()
// 	if err != nil {
// 		t.Fatalf("Failed to load test data: %v", err)
// 	}

// 	t.Logf("Loaded %d test vectors and %d neighbor lists", len(testVectors), len(neighbors))

// 	// Print first 5 dimensions and neighbors of first and second test vectors
// 	if len(testVectors) >= 2 {
// 		t.Logf("Test vector 1 first 5 dimensions: %v", testVectors[0][:5])
// 		if len(neighbors) > 0 {
// 			t.Logf("Test vector 5 neighbors: %v", neighbors[0])
// 		}

// 		t.Logf("Test vector 2 first 5 dimensions: %v", testVectors[1][:5])
// 		if len(neighbors) > 1 {
// 			t.Logf("Test vector 5 neighbors: %v", neighbors[1])
// 		}
// 	}
// }
