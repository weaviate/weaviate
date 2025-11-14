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

package clients

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// createRetryerWithTinyBackoff returns a retryer configured for fast tests
func createRetryerWithTinyBackoff() *retryer {
	r := newRetryer()
	r.minBackOff = time.Millisecond
	r.maxBackOff = time.Millisecond
	return r
}

func TestRetry_SuccessStopsImmediately(t *testing.T) {
	ctx := context.Background()
	r := createRetryerWithTinyBackoff()
	var calls int32

	work := func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		return true, nil // err == nil should stop immediately
	}

	err := r.retry(ctx, 5, work)
	require.NoError(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestRetry_PermanentErrorStopsImmediately(t *testing.T) {
	ctx := context.Background()
	r := createRetryerWithTinyBackoff()
	var calls int32
	permErr := errors.New("permanent error")

	work := func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		return false, permErr // keepTrying == false => do not retry
	}

	err := r.retry(ctx, 5, work)
	require.ErrorIs(t, err, permErr)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestRetry_RetryThenSuccess(t *testing.T) {
	ctx := context.Background()
	r := createRetryerWithTinyBackoff()
	var calls int32
	var attemptsBeforeSuccess int32 = 3

	work := func(ctx context.Context) (bool, error) {
		c := atomic.AddInt32(&calls, 1)
		if c <= attemptsBeforeSuccess {
			return true, errors.New("transient") // ask to retry
		}
		return true, nil // success
	}

	err := r.retry(ctx, 10, work)
	require.NoError(t, err)
	require.Equal(t, attemptsBeforeSuccess+1, atomic.LoadInt32(&calls))
}

func TestRetry_ExhaustsAndReturnsLastError(t *testing.T) {
	ctx := context.Background()
	r := createRetryerWithTinyBackoff()
	// Keep retries extremely short to finish quickly
	n := 1 // MaxElapsedTime = n * maxBackOff = 1ms
	var calls int32
	lastErr := errors.New("still failing")

	work := func(ctx context.Context) (bool, error) {
		atomic.AddInt32(&calls, 1)
		return true, lastErr // always request retry with an error
	}

	start := time.Now()
	err := r.retry(ctx, n, work)
	elapsed := time.Since(start)

	// Should stop with the last error after exceeding MaxElapsedTime
	require.ErrorIs(t, err, lastErr)
	// Should perform at least one call
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1))
	// Should complete quickly given tiny backoff
	require.Less(t, elapsed, time.Second)
}
