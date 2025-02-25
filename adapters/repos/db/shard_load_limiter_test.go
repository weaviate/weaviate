package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestNewShardLoadLimiter_DefaultLimit(t *testing.T) {
	tests := []struct {
		name          string
		limit         int
		expectedLimit int64
	}{
		{
			name:          "with custom limit",
			limit:         100,
			expectedLimit: 100,
		},
		{
			name:          "with default limit",
			limit:         0,
			expectedLimit: defaultShardLoadingLimit,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limiter := NewShardLoadLimiter(monitoring.NoopRegisterer, tc.limit)

			var count int64
			for limiter.sema.TryAcquire(1) {
				count++
			}

			require.Equal(t, tc.expectedLimit, count)
		})
	}
}

func TestNewShardLoadLimiter_ControlsConcurrency(t *testing.T) {
	var (
		limiter = NewShardLoadLimiter(monitoring.NoopRegisterer, 5)
		start   = time.Now()
	)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			require.NoError(t, limiter.Acquire(context.Background()))
			defer limiter.Release()

			time.Sleep(100 * time.Millisecond)
		}()
	}
	wg.Wait()

	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
}
