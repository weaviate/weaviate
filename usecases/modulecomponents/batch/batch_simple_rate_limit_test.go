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

package batch

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNewBatchSimple_RateLimiterFromRPM(t *testing.T) {
	l, _ := test.NewNullLogger()
	tests := []struct {
		name          string
		rpm           int
		expectLimiter bool
		expectedLimit rate.Limit
		expectedBurst int
	}{
		{name: "zero rpm disables limiter", rpm: 0, expectLimiter: false},
		{name: "negative rpm disables limiter", rpm: -1, expectLimiter: false},
		{name: "small rpm rounds burst up to 1", rpm: 30, expectLimiter: true, expectedLimit: rate.Limit(0.5), expectedBurst: 1},
		{name: "60 rpm yields one per second", rpm: 60, expectLimiter: true, expectedLimit: rate.Limit(1.0), expectedBurst: 1},
		{name: "600 rpm yields 10 per second", rpm: 600, expectLimiter: true, expectedLimit: rate.Limit(10.0), expectedBurst: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := NewBatchSimple[[]float32](l, tt.rpm)
			if !tt.expectLimiter {
				require.Nil(t, sb.rateLimiter)
				return
			}
			require.NotNil(t, sb.rateLimiter)
			assert.InDelta(t, float64(tt.expectedLimit), float64(sb.rateLimiter.Limit()), 1e-9)
			assert.Equal(t, tt.expectedBurst, sb.rateLimiter.Burst())
		})
	}
}

func TestBatchSimple_WaitForQuota(t *testing.T) {
	l, _ := test.NewNullLogger()
	tests := []struct {
		name        string
		limiter     *rate.Limiter
		n           int
		ctxTimeout  time.Duration
		expectError bool
	}{
		{
			name:    "nil limiter is a no-op",
			limiter: nil,
			n:       1000,
		},
		{
			name:    "request fitting in burst",
			limiter: rate.NewLimiter(rate.Limit(1000), 100),
			n:       50,
		},
		{
			name:    "request larger than burst is chunked",
			limiter: rate.NewLimiter(rate.Limit(1000), 10),
			n:       150,
		},
		{
			name:        "context deadline while waiting",
			limiter:     rate.NewLimiter(rate.Limit(1), 1),
			n:           10,
			ctxTimeout:  10 * time.Millisecond,
			expectError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()
			}
			sb := &BatchSimple[[]float32]{logger: l, rateLimiter: tt.limiter}
			err := sb.waitForQuota(ctx, tt.n)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBatchSimple_RateLimiterPacing(t *testing.T) {
	// rpm of 600 = 10 embeddings/second. After draining the burst, 3 more
	// single-input waits should take at least ~200ms.
	l, _ := test.NewNullLogger()
	sb := NewBatchSimple[[]float32](l, 600)
	require.NotNil(t, sb.rateLimiter)
	require.NoError(t, sb.waitForQuota(context.Background(), sb.rateLimiter.Burst())) // drain burst

	start := time.Now()
	for range 3 {
		require.NoError(t, sb.waitForQuota(context.Background(), 1))
	}
	assert.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
}
