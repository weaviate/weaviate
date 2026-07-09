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

package clusterapi_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

// prefilterClient is the subset of the REST replication client the round-trip tests
// exercise; it lets the helper return the unexported concrete client via an interface.
type prefilterClient interface {
	CompareHashTreeRoots(ctx context.Context, host, index string, roots map[string]hashtree.Digest) ([]string, error)
}

func newPrefilterTestServer(t *testing.T, replicator replicaTypes.Replicator) (prefilterClient, string) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	indices := clusterapi.NewReplicatedIndices(
		replicator,
		clusterapi.NewNoopAuthHandler(),
		func() bool { return false },
		logger,
		func() bool { return true },
	)
	mux := http.NewServeMux()
	mux.Handle("/replicas/indices/", indices.Indices())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client, err := clients.NewReplicationClient(&http.Client{})
	require.NoError(t, err)
	return client, strings.TrimPrefix(server.URL, "http://")
}

// TestCompareHashTreeRootsRESTRoundTrip proves the real REST client+handler report exactly the diverging shards and preserve digests bit-for-bit.
func TestCompareHashTreeRootsRESTRoundTrip(t *testing.T) {
	const index = "MyClass"

	d := func(hi, lo uint64) hashtree.Digest { return hashtree.Digest{hi, lo} }
	sourceRoots := map[string]hashtree.Digest{
		"in-sync":   d(0xFFFFFFFFFFFFFFFF, 1),
		"also-sync": d(0, 0),
		"diverging": d(2, 0xDEADBEEFCAFEBABE),
	}
	localRoots := map[string]hashtree.Digest{
		"in-sync":   d(0xFFFFFFFFFFFFFFFF, 1),
		"also-sync": d(0, 0),
		"diverging": d(9, 9),
	}

	mockReplicator := replicaTypes.NewMockReplicator(t)
	var received map[string]hashtree.Digest
	mockReplicator.EXPECT().
		CompareHashTreeRoots(mock.Anything, index, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, roots map[string]hashtree.Digest) ([]string, error) {
			received = roots
			var diverging []string
			for shard, root := range roots {
				if local, ok := localRoots[shard]; !ok || local != root {
					diverging = append(diverging, shard)
				}
			}
			return diverging, nil
		})

	client, host := newPrefilterTestServer(t, mockReplicator)

	diverging, err := client.CompareHashTreeRoots(context.Background(), host, index, sourceRoots)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"diverging"}, diverging)
	assert.Equal(t, sourceRoots, received, "handler must receive the exact source digests")
}

func TestCompareHashTreeRootsRESTRejectsOverCap(t *testing.T) {
	const index = "MyClass"

	// No expectation is set: the handler must reject before reaching the replicator.
	mockReplicator := replicaTypes.NewMockReplicator(t)
	client, host := newPrefilterTestServer(t, mockReplicator)

	roots := make(map[string]hashtree.Digest, replica.CompareHashTreeRootsMaxShardsPerRequest+1)
	for i := 0; i <= replica.CompareHashTreeRootsMaxShardsPerRequest; i++ {
		roots[fmt.Sprintf("shard-%d", i)] = hashtree.Digest{uint64(i), 0}
	}

	_, err := client.CompareHashTreeRoots(context.Background(), host, index, roots)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many shards")
}
