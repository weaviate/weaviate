//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package segmentindex

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTreeHang(t *testing.T) {
	t.Log("Creating tree with size 100...")
	tree := NewTree(100)
	t.Log("Tree created successfully")
	t.Log("Inserting one key...")
	tree.Insert([]byte("test"), 0, 50)
	t.Log("Done")
}

// BenchmarkDiskTree_AllKeys benchmarks the AllKeys() function with various tree sizes
// to validate the capacity pre-allocation optimization.
func BenchmarkDiskTree_AllKeys(b *testing.B) {
	sizes := []int{
		10000,  // large segment
		100000, // very large segment
		// 500000,  // typical production segment
		// 1000000, // large production segment
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			nodes := make(Nodes, size)
			for i := 0; i < size; i++ {
				nodes[i] = Node{
					Key:   []byte(fmt.Sprintf("key-%010d", i)),
					Start: uint64(i * 100),
					End:   uint64(i*100 + 50),
				}
			}

			// Build balanced tree
			tree := NewBalanced(nodes)

			// Marshal to create DiskTree
			bytes, err := tree.MarshalBinary()
			if err != nil {
				b.Fatalf("failed to marshal tree: %v", err)
			}

			dTree := NewDiskTree(bytes)

			// Reset timer after setup
			b.ResetTimer()

			// Run benchmark
			for i := 0; i < b.N; i++ {
				keys, err := dTree.AllKeys()
				require.NoError(b, err)
				require.Len(b, keys, size)
			}

			// Report allocations
			b.ReportAllocs()
		})
	}
}

// BenchmarkDiskTree_AllKeys_Realistic benchmarks AllKeys() with realistic key distributions
// similar to what would be seen in production (variable key sizes, mixed data).
func BenchmarkDiskTree_AllKeys_Realistic(b *testing.B) {
	// Simulate a realistic segment with 500K keys of varying sizes
	const numKeys = 5000

	tree := NewTree(4)

	// Add keys with realistic size distribution:
	// - 40% short keys (10-20 bytes) - IDs, timestamps
	// - 40% medium keys (20-50 bytes) - UUIDs, compound keys
	// - 20% long keys (50-100 bytes) - URLs, long identifiers
	for i := 0; i < numKeys; i++ {
		var key []byte
		switch {
		case i < numKeys*40/100:
			// Short keys
			key = []byte(fmt.Sprintf("id-%d", i))
		case i < numKeys*80/100:
			// Medium keys
			key = []byte(fmt.Sprintf("uuid-%d-%d-%d", i, i*2, i*3))
		default:
			// Long keys
			key = []byte(fmt.Sprintf("long-identifier-with-path-%d/%d/%d/%d", i, i*2, i*3, i*4))
		}
		tree.Insert(key, uint64(i*100), uint64(i*100+50))
	}

	// Marshal to create DiskTree
	bytes, err := tree.MarshalBinary()
	if err != nil {
		b.Fatalf("failed to marshal tree: %v", err)
	}

	dTree := NewDiskTree(bytes)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keys, err := dTree.AllKeys()
		if err != nil {
			b.Fatalf("AllKeys failed: %v", err)
		}
		if len(keys) != numKeys {
			b.Fatalf("expected %d keys, got %d", numKeys, len(keys))
		}
	}

	b.ReportAllocs()
}

// BenchmarkDiskTree_AllKeys_Memory measures memory allocations specifically
func BenchmarkDiskTree_AllKeys_Memory(b *testing.B) {
	sizes := []int{10000, 100000, 500000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			tree := NewTree(4)
			for i := 0; i < size; i++ {
				key := []byte(fmt.Sprintf("key-%010d", i))
				tree.Insert(key, uint64(i*100), uint64(i*100+50))
			}

			bytes, err := tree.MarshalBinary()
			if err != nil {
				b.Fatalf("failed to marshal tree: %v", err)
			}

			dTree := NewDiskTree(bytes)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				keys, err := dTree.AllKeys()
				if err != nil {
					b.Fatalf("AllKeys failed: %v", err)
				}
				// Force keys to be used to prevent optimization
				if len(keys) == 0 {
					b.Fatal("no keys returned")
				}
			}
		})
	}
}
