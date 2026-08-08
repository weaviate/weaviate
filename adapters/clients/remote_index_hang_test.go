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

package clients

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clusterapi "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type hangTestSchema struct{ replicas []string }

func (s hangTestSchema) ShardOwner(class, shard string) (string, error) { return s.replicas[0], nil }
func (s hangTestSchema) ShardReplicas(class, shard string) ([]string, error) {
	return s.replicas, nil
}

type hangTestResolver struct{ hosts map[string]string }

func (r hangTestResolver) NodeHostname(nodeName string) (string, bool) {
	h, ok := r.hosts[nodeName]
	return h, ok
}

func newHangAndHealthyRemoteIndex(t *testing.T) (*sharding.RemoteIndex, *atomic.Int32, *atomic.Int32) {
	t.Helper()

	var hangCalls, healthyCalls atomic.Int32

	hangRelease := make(chan struct{})
	hangSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hangCalls.Add(1)
		select {
		case <-hangRelease:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(hangSrv.Close)
	t.Cleanup(func() { close(hangRelease) })

	healthySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		healthyCalls.Add(1)
		body, err := clusterapi.IndicesPayloads.SearchResults.MarshalWithAdditional(nil, nil, additional.Properties{}, nil)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		clusterapi.IndicesPayloads.SearchResults.SetContentTypeHeader(w)
		w.Write(body)
	}))
	t.Cleanup(healthySrv.Close)

	cl := newRemoteIndex(&http.Client{})
	ri := sharding.NewRemoteIndex("C",
		hangTestSchema{replicas: []string{"N0", "N1"}},
		hangTestResolver{hosts: map[string]string{
			"N0": strings.TrimPrefix(hangSrv.URL, "http://"),
			"N1": strings.TrimPrefix(healthySrv.URL, "http://"),
		}},
		cl, nil, nil)
	return ri, &hangCalls, &healthyCalls
}

// TestRemoteIndexSearchShardHangingReplicaFailover pins the tail-latency symptom: a hung replica burns the full inner client timeout before sequential failover succeeds.
func TestRemoteIndexSearchShardHangingReplicaFailover(t *testing.T) {
	ri, hangCalls, healthyCalls := newHangAndHealthyRemoteIndex(t)
	innerTimeout := 20 * time.Millisecond * QUERY_TIMEOUT_VALUE

	sawHungFirst := false
	for i := 0; i < 50 && !sawHungFirst; i++ {
		hangBefore, healthyBefore := hangCalls.Load(), healthyCalls.Load()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		start := time.Now()
		_, _, _, node, err := ri.SearchShard(ctx, "S", nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if node != "N1" {
			continue
		}
		if hangCalls.Load() == hangBefore {
			if healthyCalls.Load() != healthyBefore+1 {
				t.Fatalf("healthy-first iteration: want exactly one healthy call, got %d", healthyCalls.Load()-healthyBefore)
			}
			continue
		}
		sawHungFirst = true
		if elapsed < innerTimeout {
			t.Fatalf("failover happened before the inner client timeout: %v < %v", elapsed, innerTimeout)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("failover took unexpectedly long: %v", elapsed)
		}
	}
	if !sawHungFirst {
		t.Fatal("random start never selected the hung replica first in 50 iterations")
	}
}

// TestRemoteIndexSearchShardHangingReplicaShortDeadline pins the DEADLINE_EXCEEDED symptom: with an outer deadline below the inner client timeout, the healthy replica is never tried.
func TestRemoteIndexSearchShardHangingReplicaShortDeadline(t *testing.T) {
	ri, hangCalls, healthyCalls := newHangAndHealthyRemoteIndex(t)

	sawHungFirst := false
	for i := 0; i < 50 && !sawHungFirst; i++ {
		hangBefore, healthyBefore := hangCalls.Load(), healthyCalls.Load()

		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		_, _, _, _, err := ri.SearchShard(ctx, "S", nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
		cancel()

		if hangCalls.Load() == hangBefore {
			if err != nil {
				t.Fatalf("healthy-first iteration: unexpected error: %v", err)
			}
			continue
		}
		sawHungFirst = true
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("hung-first iteration: want context.DeadlineExceeded, got %v", err)
		}
		if got := healthyCalls.Load() - healthyBefore; got != 0 {
			t.Fatalf("hung-first iteration: healthy replica was tried %d times despite expired deadline", got)
		}
	}
	if !sawHungFirst {
		t.Fatal("random start never selected the hung replica first in 50 iterations")
	}
}
