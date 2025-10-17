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

package batch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_statsUpdateBatchSize(t *testing.T) {
	t.Run("calculate new batch size when processing time is ideal", func(t *testing.T) {
		queue := NewProcessingQueue(1)
		stats := newStats() // default batch size is 100
		stats.updateBatchSize(time.Second, cap(queue))
		require.Equal(t, 100, stats.getBatchSize())
		// when it takes 1s to process, the batch size should remain the same
	})

	t.Run("increase batch size when processing time is lesser in value than ideal", func(t *testing.T) {
		queue := NewProcessingQueue(1)
		stats := newStats() // default batch size is 100
		stats.updateBatchSize(500*time.Millisecond, cap(queue))
		require.Greater(t, stats.getBatchSize(), 100)
		// when it takes less than 1s to process, the batch size should increase
	})

	t.Run("decrease batch size when processing time is greater in value than ideal", func(t *testing.T) {
		queue := NewProcessingQueue(1)
		stats := newStats() // default batch size is 100
		stats.updateBatchSize(2*time.Second, cap(queue))
		require.Less(t, stats.getBatchSize(), 100)
		// when it takes more than 1s to process, the batch size should decrease
	})

	t.Run("batch size should not go below 10", func(t *testing.T) {
		queue := NewProcessingQueue(1)
		stats := newStats() // default batch size is 100
		for i := 0; i < 100; i++ {
			stats.updateBatchSize(10*time.Second, cap(queue))
		}
		require.Equal(t, 10, stats.getBatchSize())
		// when it takes more than 1s to process, the batch size should decrease, but not below 10
	})

	t.Run("batch size should not go above 1000", func(t *testing.T) {
		queue := NewProcessingQueue(1)
		stats := newStats() // default batch size is 100
		for i := 0; i < 100; i++ {
			stats.updateBatchSize(10*time.Millisecond, cap(queue))
		}
		require.Equal(t, 1000, stats.getBatchSize())
		// when it takes less than 1s to process, the batch size should increase, but not above 1000
	})
}
