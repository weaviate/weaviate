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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestQueryReplicasHedgedLazyLoadingReplica pins that hedging bounds a lazily
// unloaded replica in both of its observable behaviors: blocking on an inline
// load, and rejecting the read as not ready.
func TestQueryReplicasHedgedLazyLoadingReplica(t *testing.T) {
	t.Run("replica blocking on an inline load is out-raced by the hedge", func(t *testing.T) {
		loadDone := make(chan struct{})
		defer close(loadDone)

		var calls atomic.Int32
		var loadingNode atomic.Value
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			if calls.Add(1) == 1 {
				loadingNode.Store(node)
				select {
				case <-loadDone:
					return node, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return node, nil
		}

		resolver := newFakeResolver(0, 2)
		schema := newFakeSchema(0, 2)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, node, err := rindex.queryReplicas(ctx, "S", 20*time.Millisecond, do)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result")
		}
		if node == loadingNode.Load().(string) {
			t.Fatalf("the loading replica %s won; the hedge never fired", node)
		}
		if n := calls.Load(); n != 2 {
			t.Fatalf("expected 2 replica calls (1 loading + 1 hedged), got %d", n)
		}
	})

	t.Run("replica rejecting the read as not ready fails over without waiting for the hedge timer", func(t *testing.T) {
		var calls atomic.Int32
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			if calls.Add(1) == 1 {
				return nil, enterrors.NewErrUnprocessable(fmt.Errorf("local %s shard is not ready", "S"))
			}
			return node, nil
		}

		resolver := newFakeResolver(0, 2)
		schema := newFakeSchema(0, 2)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, _, err := rindex.queryReplicas(ctx, "S", 10*time.Second, do)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result")
		}
		if n := calls.Load(); n != 2 {
			t.Fatalf("expected 2 replica calls (1 rejected + 1 fast-failure launch), got %d", n)
		}
	})
}
