package batch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_statsUpdateBatchSize(t *testing.T) {
	t.Run("calculate new batch size when processing time is ideal", func(t *testing.T) {
		queue := NewBatchProcessingQueue(100)
		stats := newStats() // default batch size is 100
		stats.updateBatchSize(time.Second, len(queue))
		require.Equal(t, 100, stats.getBatchSize())
		// when it takes 1s to process, the batch size should remain the same
	})

	t.Run("increase batch size when processing time is lesser in value than ideal", func(t *testing.T) {
		queue := NewBatchProcessingQueue(100)
		stats := newStats() // default batch size is 100
		stats.updateBatchSize(500*time.Millisecond, len(queue))
		require.Greater(t, stats.getBatchSize(), 100)
		// when it takes less than 1s to process, the batch size should increase
	})

	t.Run("decrease batch size when processing time is greater in value than ideal", func(t *testing.T) {
		queue := NewBatchProcessingQueue(100)
		stats := newStats() // default batch size is 100
		stats.updateBatchSize(2*time.Second, len(queue))
		require.Less(t, stats.getBatchSize(), 100)
		// when it takes more than 1s to process, the batch size should decrease
	})

	t.Run("batch size should not go below 10", func(t *testing.T) {
		queue := NewBatchProcessingQueue(100)
		stats := newStats() // default batch size is 100
		for i := 0; i < 100; i++ {
			stats.updateBatchSize(10*time.Second, len(queue))
		}
		require.Equal(t, 10, stats.getBatchSize())
		// when it takes more than 1s to process, the batch size should decrease, but not below 10
	})

	t.Run("batch size should not go above 1000", func(t *testing.T) {
		queue := NewBatchProcessingQueue(100)
		stats := newStats() // default batch size is 100
		for i := 0; i < 100; i++ {
			stats.updateBatchSize(10*time.Millisecond, len(queue))
		}
		require.Equal(t, 1000, stats.getBatchSize())
		// when it takes less than 1s to process, the batch size should increase, but not above 1000
	})
}
