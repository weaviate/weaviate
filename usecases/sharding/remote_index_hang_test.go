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

package sharding

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestQueryReplicasSequentialHangShortDeadline pins the sequential-path flaw hedging addresses: a hung first replica eats the outer deadline and the healthy fallback is never tried.
func TestQueryReplicasSequentialHangShortDeadline(t *testing.T) {
	resolver := newFakeResolver(0, 2)
	schema := newFakeSchema(0, 2)
	rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

	var mu sync.Mutex
	var callLog []string
	do := func(ctx context.Context, node, host string) (interface{}, error) {
		mu.Lock()
		callLog = append(callLog, node)
		mu.Unlock()
		if node == "N0" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return node, nil
	}

	sawHungFirst := false
	for i := 0; i < 50 && !sawHungFirst; i++ {
		mu.Lock()
		callLog = nil
		mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		start := time.Now()
		got, _, err := rindex.queryReplicas(ctx, "S", 0, do)
		elapsed := time.Since(start)
		cancel()

		mu.Lock()
		log := append([]string(nil), callLog...)
		mu.Unlock()

		if len(log) > 0 && log[0] == "N0" {
			sawHungFirst = true
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("hung-first iteration: want context.DeadlineExceeded, got %v", err)
			}
			if got != nil {
				t.Fatalf("hung-first iteration: want nil result, got %v", got)
			}
			if elapsed < 200*time.Millisecond {
				t.Fatalf("hung-first iteration: returned before the outer deadline (%v)", elapsed)
			}
			if len(log) != 1 {
				t.Fatalf("hung-first iteration: healthy replica was tried, call log %v", log)
			}
		} else {
			if err != nil {
				t.Fatalf("healthy-first iteration: unexpected error: %v", err)
			}
		}
	}
	if !sawHungFirst {
		t.Fatal("random start never selected the hung replica first in 50 iterations")
	}
}

// TestQueryReplicasSequentialHangFailoverLatency pins the tail-latency symptom: with a generous deadline the hung attempt burns its full budget before failover succeeds.
func TestQueryReplicasSequentialHangFailoverLatency(t *testing.T) {
	innerTimeout := 300 * time.Millisecond
	resolver := newFakeResolver(0, 2)
	schema := newFakeSchema(0, 2)
	rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

	var mu sync.Mutex
	var firstNode string
	do := func(ctx context.Context, node, host string) (interface{}, error) {
		mu.Lock()
		if firstNode == "" {
			firstNode = node
		}
		mu.Unlock()
		if node == "N0" {
			select {
			case <-time.After(innerTimeout):
				return nil, errAny
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return node, nil
	}

	sawHungFirst, sawHealthyFirst := false, false
	for i := 0; i < 50 && (!sawHungFirst || !sawHealthyFirst); i++ {
		mu.Lock()
		firstNode = ""
		mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		start := time.Now()
		got, node, err := rindex.queryReplicas(ctx, "S", 0, do)
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result")
		}

		mu.Lock()
		first := firstNode
		mu.Unlock()

		if first == "N0" {
			sawHungFirst = true
			if node != "N1" {
				t.Fatalf("hung-first iteration: want winner N1, got %s", node)
			}
			if elapsed < innerTimeout {
				t.Fatalf("hung-first iteration: failover happened before the inner timeout (%v)", elapsed)
			}
		} else {
			sawHealthyFirst = true
			if elapsed > 150*time.Millisecond {
				t.Fatalf("healthy-first iteration: unexpectedly slow (%v)", elapsed)
			}
		}
	}
	if !sawHungFirst || !sawHealthyFirst {
		t.Fatalf("did not observe both orderings in 50 iterations (hungFirst=%v healthyFirst=%v)", sawHungFirst, sawHealthyFirst)
	}
}
