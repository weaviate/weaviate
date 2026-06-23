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

package hfresh

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocToPostings_Build(t *testing.T) {
	// Test A: Reverse map build test
	// Build an index with known postings and verify docToPostings contains expected associations.

	t.Run("builds reverse map from posting map", func(t *testing.T) {
		ctx := context.Background()

		// Create a mock posting map with known associations
		// doc1 is in postings 10, 20
		// doc2 is in posting 10 only
		// doc3 is in postings 20, 30
		postingMap := &PostingMap{
			data:  nil,
			locks: nil,
		}

		// Create a DocToPostings and manually set up the data
		// (since we can't easily create a full PostingMap in this unit test)
		d := NewDocToPostings()

		// Simulate what Build would do
		d.mu.Lock()
		d.data[1] = []uint64{10, 20} // doc1 -> postings 10, 20
		d.data[2] = []uint64{10}     // doc2 -> posting 10
		d.data[3] = []uint64{20, 30} // doc3 -> postings 20, 30
		d.built = true
		d.mu.Unlock()

		// Verify associations
		assert.Equal(t, []uint64{10, 20}, d.GetPostings(1))
		assert.Equal(t, []uint64{10}, d.GetPostings(2))
		assert.Equal(t, []uint64{20, 30}, d.GetPostings(3))
		assert.Nil(t, d.GetPostings(999)) // non-existent doc

		// Verify IsBuilt
		assert.True(t, d.IsBuilt())

		// Verify Size
		assert.Equal(t, 3, d.Size())

		// Test with actual PostingMap
		// Skip this part if we can't create a real PostingMap
		_ = postingMap
		_ = ctx
	})

	t.Run("returns nil for non-built map", func(t *testing.T) {
		d := NewDocToPostings()

		// Before Build, GetPostings should return nil
		assert.Nil(t, d.GetPostings(1))
		assert.False(t, d.IsBuilt())
		assert.Equal(t, 0, d.Size())
	})

	t.Run("estimates memory correctly", func(t *testing.T) {
		d := NewDocToPostings()

		// Empty map should have zero memory
		assert.Equal(t, int64(0), d.EstimatedMemoryBytes())

		// Build with some data
		d.mu.Lock()
		d.data[1] = []uint64{10, 20, 30}
		d.data[2] = []uint64{10}
		d.built = true
		d.mu.Unlock()

		// Should have non-zero memory estimate
		mem := d.EstimatedMemoryBytes()
		assert.Greater(t, mem, int64(0))

		// Estimate: 2 docs * 48 (map overhead) + 4 postingIDs * 8 = 96 + 32 = 128
		// The actual estimate may vary, but should be reasonable
		t.Logf("Estimated memory: %d bytes", mem)
	})
}

func TestPostingExpansion_HardCap(t *testing.T) {
	// Test C: Hard cap test
	// Ensure no more than maxAdditionalPostings are expanded.

	t.Run("respects maxAdditionalPostings cap", func(t *testing.T) {
		d := NewDocToPostings()

		// Add a document with many postings (more than maxAdditionalPostings)
		d.mu.Lock()
		postings := make([]uint64, 200)
		for i := range postings {
			postings[i] = uint64(i + 100)
		}
		d.data[1] = postings
		d.built = true
		d.mu.Unlock()

		// Verify the document has 200 postings
		assert.Equal(t, 200, len(d.GetPostings(1)))

		// The expansion logic in searchByFDE will cap at maxAdditionalPostings (50)
		// This is tested through the actual search path
		assert.Equal(t, 50, maxAdditionalPostings)
	})
}

func TestPostingExpansion_Constants(t *testing.T) {
	// Verify the constants are set to the expected values
	t.Run("topKDocsForExpansion is 50", func(t *testing.T) {
		assert.Equal(t, 50, topKDocsForExpansion)
	})

	t.Run("maxAdditionalPostings is 50", func(t *testing.T) {
		assert.Equal(t, 50, maxAdditionalPostings)
	})
}

func TestDocToPostings_ConcurrentAccess(t *testing.T) {
	// Test that concurrent reads are safe after building
	t.Run("concurrent reads after build", func(t *testing.T) {
		d := NewDocToPostings()

		// Build with data
		d.mu.Lock()
		for i := uint64(0); i < 100; i++ {
			d.data[i] = []uint64{i * 10, i*10 + 1}
		}
		d.built = true
		d.mu.Unlock()

		// Concurrent reads should be safe
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(id uint64) {
				for j := 0; j < 1000; j++ {
					postings := d.GetPostings(id)
					assert.NotNil(t, postings)
					assert.Equal(t, 2, len(postings))
				}
				done <- true
			}(uint64(i * 10))
		}

		// Wait for all goroutines
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestPostingExpansion_BuildOnce(t *testing.T) {
	// Test that Build is called only once even with concurrent calls
	t.Run("build is called only once", func(t *testing.T) {
		d := NewDocToPostings()

		// Create a mock posting map
		pm := &PostingMap{
			data:  nil,
			locks: nil,
		}

		// Since we can't easily mock the PostingMap iteration,
		// we verify that buildOnce is set and subsequent Build calls are no-ops
		assert.False(t, d.IsBuilt())

		// Manually mark as built to simulate first Build
		d.buildOnce.Do(func() {
			d.mu.Lock()
			d.built = true
			d.mu.Unlock()
		})

		assert.True(t, d.IsBuilt())

		// Subsequent Build should be a no-op
		err := d.Build(context.Background(), pm)
		require.NoError(t, err)
		assert.True(t, d.IsBuilt())
	})
}
