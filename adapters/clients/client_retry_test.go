//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"context"
	"errors"
	"testing"
	"time"
)

// helper to create a retryer with large backoff to detect immediate return
func newTestRetryer() *retryer {
	return &retryer{
		minBackOff:  time.Second,
		maxBackOff:  time.Second,
		timeoutUnit: time.Millisecond,
	}
}

func TestRetryerImmediateReturnOnContextCanceled(t *testing.T) {
	r := newTestRetryer()
	ctx := context.Background()

	calls := 0
	work := func(ctx context.Context) (bool, error) {
		calls++
		return true, context.Canceled
	}

	start := time.Now()
	err := r.retry(ctx, 9, work)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 work call, got %d", calls)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected immediate return without backoff, took %v", elapsed)
	}
}

func TestRetryerImmediateReturnOnDeadlineExceeded(t *testing.T) {
	r := newTestRetryer()
	ctx := context.Background()

	calls := 0
	work := func(ctx context.Context) (bool, error) {
		calls++
		return true, context.DeadlineExceeded
	}

	start := time.Now()
	err := r.retry(ctx, 9, work)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 work call, got %d", calls)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected immediate return without backoff, took %v", elapsed)
	}
}
