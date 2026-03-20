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

package distributedtask

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimiter_BasicAcquireRelease(t *testing.T) {
	l := NewConcurrencyLimiter(2)

	require.NoError(t, l.Acquire(context.Background()))
	require.NoError(t, l.Acquire(context.Background()))

	// Third acquire should block. Use a cancelled context to verify.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := l.Acquire(ctx)
	assert.ErrorIs(t, err, context.Canceled)

	// Release one slot, then acquire should succeed.
	l.Release()
	require.NoError(t, l.Acquire(context.Background()))
	l.Release()
	l.Release()
}

func TestConcurrencyLimiter_ConcurrentAccess(t *testing.T) {
	const maxConcurrency = 4
	const totalWorkers = 20

	l := NewConcurrencyLimiter(maxConcurrency)
	var concurrent atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < totalWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, l.Acquire(context.Background()))
			defer l.Release()

			cur := concurrent.Add(1)
			// Track max concurrency observed
			for {
				old := maxObserved.Load()
				if cur <= old || maxObserved.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			concurrent.Add(-1)
		}()
	}

	wg.Wait()
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConcurrency))
	assert.Equal(t, int32(0), concurrent.Load())
}

func TestConcurrencyLimiter_ContextCancellation(t *testing.T) {
	l := NewConcurrencyLimiter(1)
	require.NoError(t, l.Acquire(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := l.Acquire(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	l.Release()
}

func TestConcurrencyLimiter_ClampsInvalidMaxToOne(t *testing.T) {
	// Values < 1 should be clamped to 1 (sequential), not panic.
	l0 := NewConcurrencyLimiter(0)
	assert.NoError(t, l0.Acquire(context.Background()))
	l0.Release()

	lNeg := NewConcurrencyLimiter(-1)
	assert.NoError(t, lNeg.Acquire(context.Background()))
	lNeg.Release()
}
