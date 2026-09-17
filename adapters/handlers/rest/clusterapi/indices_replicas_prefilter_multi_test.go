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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

type crossClassClient interface {
	CompareHashTreeRootsMulti(ctx context.Context, host string,
		classes map[string]map[string]hashtree.Digest) (*replica.CompareHashTreeRootsMultiResp, error)
}

func newMultiPrefilterTestServer(t *testing.T, replicator replicaTypes.Replicator) (crossClassClient, string) {
	t.Helper()
	client, host := newPrefilterTestServer(t, replicator)
	return client.(crossClassClient), host
}

// TestCompareHashTreeRootsMultiRESTRoundTrip proves the real REST client+handler classify per class and isolate per-class errors.
func TestCompareHashTreeRootsMultiRESTRoundTrip(t *testing.T) {
	d := func(hi, lo uint64) hashtree.Digest { return hashtree.Digest{hi, lo} }
	payload := map[string]map[string]hashtree.Digest{
		"ClassA": {"a1": d(1, 1), "a2": d(0xFFFFFFFFFFFFFFFF, 2)},
		"ClassB": {"b1": d(3, 0xDEADBEEFCAFEBABE)},
		"ClassC": {"c1": d(4, 4)},
	}

	mockReplicator := replicaTypes.NewMockReplicator(t)
	received := map[string]map[string]hashtree.Digest{}
	mockReplicator.EXPECT().
		CompareHashTreeRoots(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, class string, roots map[string]hashtree.Digest) ([]string, error) {
			received[class] = roots
			switch class {
			case "ClassA":
				return []string{"a2"}, nil
			case "ClassB":
				return nil, nil
			default:
				return nil, errors.New("index not loaded")
			}
		})

	client, host := newMultiPrefilterTestServer(t, mockReplicator)
	resp, err := client.CompareHashTreeRootsMulti(context.Background(), host, payload)
	require.NoError(t, err)
	require.Len(t, resp.Classes, 3)

	assert.Equal(t, payload, received)
	assert.Equal(t, []string{"a2"}, resp.Classes["ClassA"].DivergingShards)
	assert.Empty(t, resp.Classes["ClassA"].Error)
	assert.Empty(t, resp.Classes["ClassB"].DivergingShards)
	assert.Empty(t, resp.Classes["ClassB"].Error)
	assert.Contains(t, resp.Classes["ClassC"].Error, "index not loaded")
}

// TestCompareHashTreeRootsMultiUnsupportedPeer pins the 404→Unsupported fallback for peers without the route.
func TestCompareHashTreeRootsMultiUnsupportedPeer(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	client, err := clients.NewReplicationClient(&http.Client{})
	require.NoError(t, err)

	_, err = client.CompareHashTreeRootsMulti(context.Background(),
		strings.TrimPrefix(server.URL, "http://"),
		map[string]map[string]hashtree.Digest{"C": {"s": {1, 2}}})
	assert.ErrorIs(t, err, replica.ErrCompareHashTreeRootsUnsupported)
}

// TestCompareHashTreeRootsMultiShardCap rejects requests whose total shard count exceeds the receiver cap.
func TestCompareHashTreeRootsMultiShardCap(t *testing.T) {
	mockReplicator := replicaTypes.NewMockReplicator(t)
	client, host := newMultiPrefilterTestServer(t, mockReplicator)

	oversized := map[string]map[string]hashtree.Digest{}
	perClass := replica.CompareHashTreeRootsMaxShardsPerRequest / 4
	for c := range 5 {
		shards := map[string]hashtree.Digest{}
		for s := range perClass {
			shards[fmt.Sprintf("s%d", s)] = hashtree.Digest{uint64(c), uint64(s)}
		}
		oversized[fmt.Sprintf("Class%d", c)] = shards
	}

	_, err := client.CompareHashTreeRootsMulti(context.Background(), host, oversized)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many shards")
}
