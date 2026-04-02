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
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var errAny = errors.New("anyErr")

func TestQueryReplica(t *testing.T) {
	var (
		ctx                      = context.Background()
		canceledCtx, cancledFunc = context.WithCancel(ctx)
	)
	cancledFunc()
	doIf := func(targetNode string) func(ctx context.Context, node, host string) (interface{}, error) {
		return func(ctx context.Context, node, host string) (interface{}, error) {
			if node != targetNode {
				return nil, errAny
			}
			return node, nil
		}
	}
	tests := []struct {
		ctx        context.Context
		resolver   fakeNodeResolver
		schema     fakeSchema
		targetNode string
		success    bool
		name       string
	}{
		{
			ctx, newFakeResolver(0, 0), newFakeSchema(0, 0), "N0", false, "empty schema",
		},
		{
			ctx, newFakeResolver(0, 1), newFakeSchema(1, 2), "N2", false, "unresolved name",
		},
		{
			ctx, newFakeResolver(0, 1), newFakeSchema(0, 1), "N0", true, "one replica",
		},
		{
			ctx, newFakeResolver(0, 9), newFakeSchema(0, 9), "N2", true, "random selection",
		},
		{
			canceledCtx, newFakeResolver(0, 9), newFakeSchema(0, 9), "N2", false, "canceled",
		},
	}

	for _, test := range tests {
		rindex := RemoteIndex{class: "C", stateGetter: &test.schema, nodeResolver: &test.resolver}
		got, lastNode, err := rindex.queryReplicas(test.ctx, "S", 0, doIf(test.targetNode))
		if !test.success {
			if got != nil {
				t.Errorf("%s: want: nil, got: %v", test.name, got)
			} else if err == nil {
				t.Errorf("%s: must return an error", test.name)
			}
			continue
		}
		if lastNode != test.targetNode {
			t.Errorf("%s: last responding node want:%s got:%s", test.name, test.targetNode, lastNode)
		}
	}
}

func TestQueryReplicasHedged(t *testing.T) {
	// Returns the first replica immediately — hedge timer should never fire.
	// Verified by checking that exactly one call was made after queryReplicas
	// returns (no wall-clock timing assertions).
	t.Run("first replica responds before hedge fires", func(t *testing.T) {
		var calls atomic.Int32
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			calls.Add(1)
			return node, nil
		}
		resolver := newFakeResolver(0, 4)
		schema := newFakeSchema(0, 4)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		// Use a very large hedge delay so the timer never fires before the first
		// (immediately responding) replica completes, avoiding flakiness on loaded CI.
		got, _, err := rindex.queryReplicas(context.Background(), "S", 10*time.Second, do)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result, got nil")
		}
		// Once queryReplicas returns the hedge timer is stopped; no additional
		// goroutines will be launched, so the call count is stable.
		if n := calls.Load(); n != 1 {
			t.Errorf("expected 1 replica call, got %d (hedge fired too early)", n)
		}
	})

	// First called replica is unresponsive; after hedgeDelay the second is launched
	// and responds immediately. The blocked goroutine unblocks via hedgeCtx
	// cancellation when the winner is found, so no goroutine leak occurs.
	t.Run("hedge fires and second replica wins when first is unresponsive", func(t *testing.T) {
		hedgeDelay := 20 * time.Millisecond

		var callOrder atomic.Int32
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			if callOrder.Add(1) == 1 {
				<-ctx.Done() // ctx is hedgeCtx; cancelled when a winner is found
				return nil, ctx.Err()
			}
			return node, nil
		}

		resolver := newFakeResolver(0, 3)
		schema := newFakeSchema(0, 3)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		// Use a timeout context so the test fails fast if hedging/cancellation breaks.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		got, _, err := rindex.queryReplicas(ctx, "S", hedgeDelay, do)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result, got nil")
		}
		// The first call blocks until its context is cancelled; a second call
		// must have been started (hedge fired) to obtain the successful result.
		if n := callOrder.Load(); n != 2 {
			t.Errorf("expected 2 replica calls (hedge fired once), got %d", n)
		}
	})

	// The first N-1 called replicas are all unresponsive; each hedge fires and
	// launches the next, until the last replica responds immediately. Blocked
	// goroutines are unblocked by hedgeCtx cancellation on success.
	t.Run("multiple unresponsive replicas, last healthy one wins", func(t *testing.T) {
		hedgeDelay := 20 * time.Millisecond
		numNodes := 4

		var callOrder atomic.Int32
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			if callOrder.Add(1) < int32(numNodes) {
				<-ctx.Done() // ctx is hedgeCtx; cancelled when a winner is found
				return nil, ctx.Err()
			}
			return node, nil
		}

		resolver := newFakeResolver(0, numNodes)
		schema := newFakeSchema(0, numNodes)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		// Use a timeout context so the test fails fast if hedging/cancellation breaks.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		got, _, err := rindex.queryReplicas(ctx, "S", hedgeDelay, do)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result, got nil")
		}
		// All N replicas must have been called: N-1 hedges fired before the last one won.
		if n := callOrder.Load(); n != int32(numNodes) {
			t.Errorf("expected %d replica calls, got %d", numNodes, n)
		}
	})

	// First replica panics; the panic is recovered and forwarded as an error so
	// the fast-failure path launches the second replica immediately, which
	// succeeds. queryReplicas must not hang.
	t.Run("panic in queryOne is recovered and does not block", func(t *testing.T) {
		var calls atomic.Int32
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			if calls.Add(1) == 1 {
				panic("simulated panic in queryOne")
			}
			return node, nil
		}

		resolver := newFakeResolver(0, 3)
		schema := newFakeSchema(0, 3)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		// Large hedge delay: fast-failure path (not the timer) launches the next replica.
		got, _, err := rindex.queryReplicas(context.Background(), "S", 10*time.Second, do)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil {
			t.Fatal("expected a result, got nil")
		}
		// Panic triggers fast-failure: second replica is launched immediately.
		if n := calls.Load(); n != 2 {
			t.Errorf("expected 2 calls (1 panicking + 1 successful), got %d", n)
		}
	})

	// Every replica returns an error; queryReplicas should surface the last error.
	t.Run("all replicas fail returns error", func(t *testing.T) {
		do := func(ctx context.Context, node, host string) (interface{}, error) {
			return nil, errAny
		}
		resolver := newFakeResolver(0, 3)
		schema := newFakeSchema(0, 3)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		got, _, err := rindex.queryReplicas(context.Background(), "S", 5*time.Millisecond, do)

		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if got != nil {
			t.Errorf("expected nil result on all-fail, got %v", got)
		}
	})

	// The outer context expires before any replica responds; the context error
	// must be returned and no result produced.
	t.Run("context expires before any replica responds", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()

		do := func(ctx context.Context, node, host string) (interface{}, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}

		resolver := newFakeResolver(0, 3)
		schema := newFakeSchema(0, 3)
		rindex := RemoteIndex{class: "C", stateGetter: &schema, nodeResolver: &resolver}

		got, _, err := rindex.queryReplicas(ctx, "S", 5*time.Millisecond, do)

		if err == nil {
			t.Fatal("expected a context error, got nil")
		}
		if got != nil {
			t.Errorf("expected nil result on timeout, got %v", got)
		}
	})
}

func newFakeResolver(fromNode, toNode int) fakeNodeResolver {
	m := make(map[string]string, toNode-fromNode)
	for i := fromNode; i < toNode; i++ {
		m[fmt.Sprintf("N%d", i)] = fmt.Sprintf("H%d", i)
	}
	return fakeNodeResolver{m}
}

func newFakeSchema(fromNode, toNode int) fakeSchema {
	nodes := make([]string, 0, toNode-fromNode)
	for i := fromNode; i < toNode; i++ {
		nodes = append(nodes, fmt.Sprintf("N%d", i))
	}
	return fakeSchema{nodes}
}

type fakeNodeResolver struct {
	rTable map[string]string
}

func (r *fakeNodeResolver) AllHostnames() []string {
	hosts := make([]string, 0, len(r.rTable))

	for _, h := range r.rTable {
		hosts = append(hosts, h)
	}

	return hosts
}

func (f *fakeNodeResolver) NodeHostname(name string) (string, bool) {
	host, ok := f.rTable[name]
	return host, ok
}

type fakeSchema struct {
	nodes []string
}

func (f *fakeSchema) ShardOwner(class, shard string) (string, error) {
	return "", nil
}

func (f *fakeSchema) ShardReplicas(class, shard string) ([]string, error) {
	return f.nodes, nil
}
