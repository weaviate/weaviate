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

package datasets

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLoadTrainData(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	hf := NewHubDataset("tobias-weaviate/fiqa_gemma300m_d768_cosine", logger)
	ids, vectors, err := hf.LoadTrainData()
	if err != nil {
		t.Fatalf("Failed to load training data: %v", err)
	}

	n := 54351
	d := 768
	assert.Equal(t, len(ids), n)
	assert.Equal(t, len(vectors), n)
	assert.Equal(t, len(vectors[0]), d)
	for i := range n {
		assert.Equal(t, ids[i], uint64(i))
	}

	expectedValues := []float32{-0.0458683, -0.022633573, -0.023361705, 0.08714058}
	actualValues := vectors[8763][634:638]
	for i := range expectedValues {
		assert.Equal(t, expectedValues[i], actualValues[i])
	}
}

func TestLoadTestData(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	hf := NewHubDataset("tobias-weaviate/fiqa_gemma300m_d768_cosine", logger)
	neighbors, vectors, err := hf.LoadTestData()
	if err != nil {
		t.Fatalf("Failed to load test data: %v", err)
	}

	n := 6640
	assert.Equal(t, len(neighbors), n)
	assert.Equal(t, len(vectors), n)

	d := 768
	for _, v := range vectors {
		assert.Equal(t, d, len(v))
	}

	// Verify that the neighbors are right in a sample location.
	expectedValues := []uint64{11196, 24739, 44342}
	actualValues := neighbors[3116][34:37]
	for i := range expectedValues {
		assert.Equal(t, expectedValues[i], actualValues[i])
	}

	// Verify that vectors are correct in a sample location
	assert.Equal(t, float32(-0.1131085529923439), vectors[5001][0])
	assert.Equal(t, float32(-0.02702171355485916), vectors[5001][767])
}

func BenchmarkLoadData(b *testing.B) {
	logger := logrus.New()
	logger.SetLevel(logrus.FatalLevel)
	hf := NewHubDataset("tobias-weaviate/fiqa_gemma300m_d768_cosine", logger)
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
