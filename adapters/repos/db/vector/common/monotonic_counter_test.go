package common

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMonotonicCounter(t *testing.T) {
	t.Run("Next", func(t *testing.T) {
		c := NewUint64Counter(10)
		require.Equal(t, 11, int(c.Next()), "Next should return 11")
		require.Equal(t, 12, int(c.Next()), "Next should return 12")
	})

	t.Run("TryNext", func(t *testing.T) {
		c := NewUint64Counter(10)
		next, ok := c.TryNext()
		require.True(t, ok, "TryNext should return ok == true")
		require.Equal(t, 11, int(next), "TryNext should return 11")
		next, ok = c.TryNext()
		require.True(t, ok, "TryNext should return ok == true")
		require.Equal(t, 12, int(next), "TryNext should return 12")
	})

	t.Run("NextN", func(t *testing.T) {
		c := NewUint64Counter(10)
		start, end := c.NextN(5)
		require.Equal(t, 11, int(start), "NextN should return start 11")
		require.Equal(t, 15, int(end), "NextN should return end 15")
	})

	t.Run("TryNextN", func(t *testing.T) {
		c := NewUint64Counter(10)
		start, end, ok := c.TryNextN(5)
		require.True(t, ok, "TryNextN should return ok == true")
		require.Equal(t, 11, int(start), "TryNextN should return start 11")
		require.Equal(t, 15, int(end), "TryNextN should return end 15")

		// Test overflow
		c = NewUint64Counter(^uint64(0)) // Set to max value
		start, end, ok = c.TryNextN(1)
		require.False(t, ok, "TryNextN should return ok == false on overflow")
		require.Equal(t, uint64(0), start, "Start should be 0 on overflow")
		require.Equal(t, uint64(0), end, "End should be 0 on overflow")
	})

	t.Run("Overflow", func(t *testing.T) {
		c := NewUint64Counter(^uint64(0)) // Set to max value
		require.Panics(t, func() { c.Next() }, "Next should panic on overflow")
		require.Panics(t, func() { c.NextN(1) }, "NextN should panic on overflow")
	})

	t.Run("TryNextOverflow", func(t *testing.T) {
		c := NewUint64Counter(^uint64(0)) // Set to max value
		next, ok := c.TryNext()
		require.False(t, ok, "TryNext should return ok == false on overflow")
		require.Equal(t, uint64(0), next, "Next should be 0 on overflow")
	})
}

func TestMonotonicCounter_Parallel(t *testing.T) {
	const goroutines = 100
	const increments = 1000

	c := NewUint64Counter(0)
	var wg sync.WaitGroup
	wg.Add(goroutines)

	results := make(chan uint64, goroutines*increments)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				results <- c.Next()
			}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[uint64]bool)
	for id := range results {
		if seen[id] {
			t.Fatalf("duplicate ID detected: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != goroutines*increments {
		t.Fatalf("expected %d IDs, got %d", goroutines*increments, len(seen))
	}
}

func BenchmarkMonotonicCounter(b *testing.B) {
	c := NewUint64Counter(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Next()
	}
}
