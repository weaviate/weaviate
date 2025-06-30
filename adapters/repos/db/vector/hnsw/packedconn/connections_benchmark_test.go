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

package packedconn

import (
	"math/rand"
	"testing"
)

// Original implementation for comparison
func (c *Connections) bulkInsertAtLayerOriginal(conns []uint64, layer uint8) {
	if layer >= c.layerCount {
		c.GrowLayersTo(layer)
	}

	if len(conns) == 0 {
		return
	}

	layerData := &c.layers[layer]

	if layerData.packed == 0 {
		// Empty layer - just encode all values
		scheme := determineOptimalScheme(conns)
		layerData.packed = packSchemeAndCount(scheme, uint32(len(conns)))
		layerData.data = encodeValues(conns, scheme)
		return
	}

	// Always decode, merge, and re-encode (original behavior)
	currentScheme := unpackScheme(layerData.packed)
	existing := decodeValues(layerData.data, currentScheme, unpackCount(layerData.packed))
	all := append(existing, conns...)

	scheme := determineOptimalScheme(all)
	layerData.packed = packSchemeAndCount(scheme, uint32(len(all)))
	layerData.data = encodeValues(all, scheme)
}

// Generate test data with different value ranges
func generateTestData(size int, maxValue uint64) []uint64 {
	data := make([]uint64, size)
	for i := 0; i < size; i++ {
		data[i] = uint64(rand.Int63n(int64(maxValue)))
	}
	return data
}

// Benchmark: Same scheme scenario (most common case)
// Initial data fits in 2-byte scheme, new data also fits in 2-byte scheme
func BenchmarkBulkInsert_SameScheme_32(b *testing.B) {
	initialData := generateTestData(32, 65535) // 2-byte scheme
	newData := generateTestData(16, 65535)     // Also 2-byte scheme

	b.Run("Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.bulkInsertAtLayerOriginal(newData, 0)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.BulkInsertAtLayer(newData, 0)
		}
	})
}

func BenchmarkBulkInsert_SameScheme_64(b *testing.B) {
	initialData := generateTestData(64, 65535) // 2-byte scheme
	newData := generateTestData(32, 65535)     // Also 2-byte scheme

	b.Run("Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.bulkInsertAtLayerOriginal(newData, 0)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.BulkInsertAtLayer(newData, 0)
		}
	})
}

// Benchmark: Scheme upgrade scenario
// Initial data fits in 2-byte scheme, new data requires 4-byte scheme
func BenchmarkBulkInsert_SchemeUpgrade_32(b *testing.B) {
	initialData := generateTestData(32, 65535)  // 2-byte scheme
	newData := generateTestData(16, 4294967295) // 4-byte scheme

	b.Run("Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.bulkInsertAtLayerOriginal(newData, 0)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.BulkInsertAtLayer(newData, 0)
		}
	})
}

func BenchmarkBulkInsert_SchemeUpgrade_64(b *testing.B) {
	initialData := generateTestData(64, 65535)  // 2-byte scheme
	newData := generateTestData(32, 4294967295) // 4-byte scheme

	b.Run("Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.bulkInsertAtLayerOriginal(newData, 0)
		}
	})

	b.Run("Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.BulkInsertAtLayer(newData, 0)
		}
	})
}

// Benchmark: Different scheme combinations
func BenchmarkBulkInsert_Various_Schemes(b *testing.B) {
	scenarios := []struct {
		name        string
		initialMax  uint64
		newMax      uint64
		initialSize int
		newSize     int
	}{
		{"2byte_to_2byte_32", 65535, 65535, 32, 16},
		{"2byte_to_2byte_64", 65535, 65535, 64, 32},
		{"2byte_to_3byte_32", 65535, 16777215, 32, 16},
		{"2byte_to_3byte_64", 65535, 16777215, 64, 32},
		{"3byte_to_3byte_32", 16777215, 16777215, 32, 16},
		{"3byte_to_3byte_64", 16777215, 16777215, 64, 32},
		{"4byte_to_4byte_32", 4294967295, 4294967295, 32, 16},
		{"4byte_to_4byte_64", 4294967295, 4294967295, 64, 32},
	}

	for _, scenario := range scenarios {
		initialData := generateTestData(scenario.initialSize, scenario.initialMax)
		newData := generateTestData(scenario.newSize, scenario.newMax)

		b.Run(scenario.name+"_Original", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c, _ := NewWithMaxLayer(0)
				c.ReplaceLayer(0, initialData)
				c.bulkInsertAtLayerOriginal(newData, 0)
			}
		})

		b.Run(scenario.name+"_Optimized", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c, _ := NewWithMaxLayer(0)
				c.ReplaceLayer(0, initialData)
				c.BulkInsertAtLayer(newData, 0)
			}
		})
	}
}

// Benchmark memory allocations
func BenchmarkBulkInsert_Allocations(b *testing.B) {
	initialData := generateTestData(64, 65535) // 2-byte scheme
	newData := generateTestData(32, 65535)     // Also 2-byte scheme

	b.Run("Original_Allocs", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.bulkInsertAtLayerOriginal(newData, 0)
		}
	})

	b.Run("Optimized_Allocs", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)
			c.ReplaceLayer(0, initialData)
			c.BulkInsertAtLayer(newData, 0)
		}
	})
}

// Benchmark with realistic workload patterns
func BenchmarkBulkInsert_RealisticWorkload(b *testing.B) {
	// Simulate building a connection list incrementally
	// Most additions don't require scheme upgrades

	b.Run("Realistic_32_Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)

			// Initial small list
			initial := generateTestData(8, 65535)
			c.ReplaceLayer(0, initial)

			// Add data in chunks (realistic pattern)
			for j := 0; j < 4; j++ {
				chunk := generateTestData(8, 65535) // Same scheme
				c.bulkInsertAtLayerOriginal(chunk, 0)
			}
		}
	})

	b.Run("Realistic_32_Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)

			// Initial small list
			initial := generateTestData(8, 65535)
			c.ReplaceLayer(0, initial)

			// Add data in chunks (realistic pattern)
			for j := 0; j < 4; j++ {
				chunk := generateTestData(8, 65535) // Same scheme
				c.BulkInsertAtLayer(chunk, 0)
			}
		}
	})

	b.Run("Realistic_64_Original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)

			// Initial medium list
			initial := generateTestData(16, 65535)
			c.ReplaceLayer(0, initial)

			// Add data in chunks
			for j := 0; j < 4; j++ {
				chunk := generateTestData(12, 65535) // Same scheme
				c.bulkInsertAtLayerOriginal(chunk, 0)
			}
		}
	})

	b.Run("Realistic_64_Optimized", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c, _ := NewWithMaxLayer(0)

			// Initial medium list
			initial := generateTestData(16, 65535)
			c.ReplaceLayer(0, initial)

			// Add data in chunks
			for j := 0; j < 4; j++ {
				chunk := generateTestData(12, 65535) // Same scheme
				c.BulkInsertAtLayer(chunk, 0)
			}
		}
	})
}
