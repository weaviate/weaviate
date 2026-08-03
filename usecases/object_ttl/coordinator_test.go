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

package objectttl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// clusterClientTimeout mirrors the production MINIMUM_INTERNAL_TIMEOUT default. A scheduled
// round must never take this long just because one peer is slow to answer.
const clusterClientTimeout = 30 * time.Second

type fakeTTLSchemaReader struct{}

func (fakeTTLSchemaReader) ReadSchema(reader func(models.Class, uint64)) error {
	reader(models.Class{
		Class:           "Expiring",
		ObjectTTLConfig: &models.ObjectTTLConfig{Enabled: true, DeleteOn: "expireDate"},
	}, 7)
	return nil
}

type fakeTTLNodeLister struct {
	local string
	all   []string
}

func (f fakeTTLNodeLister) NodeName() string { return f.local }
func (f fakeTTLNodeLister) Nodes() []string  { return f.all }

type fakeNodeResolver map[string]string

func (f fakeNodeResolver) NodeHostname(node string) (string, bool) {
	host, ok := f[node]
	return host, ok
}

// unresponsivePeer accepts connections but never answers, which is how a peer that is wedged,
// starved of CPU, or not finished booting looks to the coordinator.
func unresponsivePeer(t *testing.T) string {
	t.Helper()
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-done
	}))
	t.Cleanup(func() {
		close(done)
		srv.Close()
	})
	return hostOf(t, srv)
}

// healthyPeer answers both control endpoints immediately and counts dispatched deletions.
func healthyPeer(t *testing.T, dispatched *atomic.Int32) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			writeStatus(w, false)
		case strings.HasSuffix(r.URL.Path, "/delete_expired"):
			dispatched.Add(1)
			w.WriteHeader(http.StatusAccepted)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return hostOf(t, srv)
}

func writeStatus(w http.ResponseWriter, ongoing bool) {
	status := ObjectsExpiredStatusResponse{DeletionOngoing: ongoing}
	status.SetContentTypeHeader(w)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

func hostOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return u.Host
}

func newTestCoordinator(t *testing.T, resolver fakeNodeResolver, nodes []string) *Coordinator {
	t.Helper()
	logger, _ := test.NewNullLogger()
	client := &http.Client{Timeout: clusterClientTimeout}
	return &Coordinator{
		schemaReader:    fakeTTLSchemaReader{},
		schemaGetter:    fakeTTLNodeLister{local: "leader", all: nodes},
		logger:          logger,
		clusterClient:   client,
		nodeResolver:    resolver,
		remoteObjectTTL: newRemoteObjectTTL(client, resolver),
		localStatus:     NewLocalStatus(),
	}
}

// A scheduled round talks to peers over the shared cluster client, whose only bound is
// MINIMUM_INTERNAL_TIMEOUT (30s). Rounds are serialised and the scheduler drops ticks that
// arrive while one is in flight, so any round that takes tens of seconds stops TTL deletion on
// every node for that long. These cases pin each call that a slow peer can stretch.
func TestCoordinatorRoundIsNotStalledBySlowPeer(t *testing.T) {
	// A round must finish in about the per-call bound. Allow generous slack for CI while still
	// failing loudly if it falls back to the 30s cluster client timeout.
	maxRound := 10 * time.Second

	t.Run("dispatch to an unresponsive peer", func(t *testing.T) {
		// Cold cluster: no previous node yet, so the first blocking call is the dispatch itself.
		peer := unresponsivePeer(t)
		c := newTestCoordinator(t, fakeNodeResolver{"node-a": peer}, []string{"leader", "node-a"})

		started := time.Now()
		err := c.Start(context.Background(), false, time.Now(), time.Now())
		took := time.Since(started)

		require.Error(t, err, "dispatch to an unresponsive peer should report failure")
		assert.Less(t, took, maxRound, "round blocked on an unresponsive peer for %s", took)
	})

	t.Run("status probe of an unresponsive peer", func(t *testing.T) {
		var dispatched atomic.Int32
		peer := unresponsivePeer(t)
		healthy := healthyPeer(t, &dispatched)
		c := newTestCoordinator(t, fakeNodeResolver{"node-a": peer, "node-b": healthy},
			[]string{"leader", "node-b"})
		// The previous round ran on the peer that has since gone unresponsive.
		c.objectTTLLastNode = "node-a"

		started := time.Now()
		err := c.Start(context.Background(), false, time.Now(), time.Now())
		took := time.Since(started)

		require.NoError(t, err)
		assert.Less(t, took, maxRound, "round blocked probing an unresponsive peer for %s", took)
		assert.Equal(t, int32(1), dispatched.Load(),
			"round should still dispatch to the healthy node after giving up on the probe")
	})
}

// A peer that reports an ongoing deletion must suppress the round, and only that.
func TestCoordinatorSkipsRoundWhileLastNodeIsBusy(t *testing.T) {
	var dispatched atomic.Int32
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, true)
	}))
	t.Cleanup(busy.Close)

	healthy := healthyPeer(t, &dispatched)
	c := newTestCoordinator(t, fakeNodeResolver{"node-a": hostOf(t, busy), "node-b": healthy},
		[]string{"leader", "node-b"})
	c.objectTTLLastNode = "node-a"

	require.NoError(t, c.Start(context.Background(), false, time.Now(), time.Now()))
	assert.Equal(t, int32(0), dispatched.Load(), "no new run while the previous one is ongoing")
}
