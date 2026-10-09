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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/replica"
)

const (
	versionedClass = "C1"
	versionedShard = "S1"
	versionedUUID  = strfmt.UUID("8c29da7a-600a-43b4-ab0f-9ba6e2e3d9b3")
)

// TestSchemaVersionSourceFallsBackToZero covers the window before startup wires a provider, and
// every test that builds a client directly: the receiver then sees 0 and treats any miss as lag.
func TestSchemaVersionSourceFallsBackToZero(t *testing.T) {
	var s schemaVersionSource
	assert.Equal(t, uint64(0), s.schemaVersion(versionedClass))
	assert.Equal(t, replica.SchemaVersionKey+"=0", s.schemaVersionQuery(versionedClass))

	s.SetSchemaVersionProvider(nil)
	assert.Equal(t, uint64(0), s.schemaVersion(versionedClass), "a nil provider must not be installed")

	s.SetSchemaVersionProvider(func(class string) uint64 {
		assert.Equal(t, versionedClass, class, "the provider is asked about the class being read")
		return 4711
	})
	assert.Equal(t, uint64(4711), s.schemaVersion(versionedClass))
}

// TestRemoteIndexReadsCarrySchemaVersion checks every remote read puts the version on the wire.
// Without it the receiving node has to answer "not ready" to every miss.
func TestRemoteIndexReadsCarrySchemaVersion(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		respond func(w http.ResponseWriter)
		call    func(ctx context.Context, c *RemoteIndex, host string) error
	}{
		{
			name:    "GetObject",
			path:    "/indices/C1/shards/S1/objects/" + string(versionedUUID),
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.GetObject(ctx, host, versionedClass, versionedShard, versionedUUID,
					search.SelectProperties{}, additional.Properties{})
				return err
			},
		},
		{
			name:    "Exists",
			path:    "/indices/C1/shards/S1/objects/" + string(versionedUUID),
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.Exists(ctx, host, versionedClass, versionedShard, versionedUUID)
				return err
			},
		},
		{
			name:    "MultiGetObjects",
			path:    "/indices/C1/shards/S1/objects",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.MultiGetObjects(ctx, host, versionedClass, versionedShard, nil)
				return err
			},
		},
		{
			name:    "SearchShard",
			path:    "/indices/C1/shards/S1/objects/_search",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, _, _, err := c.SearchShard(ctx, host, versionedClass, versionedShard,
					nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil)
				return err
			},
		},
		{
			name:    "Aggregate",
			path:    "/indices/C1/shards/S1/objects/_aggregations",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.Aggregate(ctx, host, versionedClass, versionedShard, aggregation.Params{})
				return err
			},
		},
		{
			name:    "FindUUIDs",
			path:    "/indices/C1/shards/S1/objects/_find",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.FindUUIDs(ctx, host, versionedClass, versionedShard, nil, 10)
				return err
			},
		},
		{
			name:    "GetShardQueueSize",
			path:    "/indices/C1/shards/S1/queuesize",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.GetShardQueueSize(ctx, host, versionedClass, versionedShard)
				return err
			},
		},
		{
			name:    "GetShardStatus",
			path:    "/indices/C1/shards/S1/status",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.GetShardStatus(ctx, host, versionedClass, versionedShard)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			serv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tt.path, r.URL.Path)
				got = r.URL.Query().Get(replica.SchemaVersionKey)
				tt.respond(w)
			}))
			defer serv.Close()

			client := newRemoteIndex(serv.Client())
			client.SetSchemaVersionProvider(func(string) uint64 { return 4711 })
			_ = tt.call(context.Background(), client, serv.URL[len("http://"):])

			assert.Equal(t, "4711", got, "read must carry %s", replica.SchemaVersionKey)
		})
	}
}

// TestReplicationReadsCarrySchemaVersion is the same check on the replica transport. FetchObjects
// is the one worth pinning: it rebuilds RawQuery for its id list, which silently dropped what the
// request builder had already put there.
func TestReplicationReadsCarrySchemaVersion(t *testing.T) {
	tests := []struct {
		name string
		path string
		call func(ctx context.Context, c *replicationClient, host string) error
	}{
		{
			name: "FetchObject",
			path: "/replicas/indices/C1/shards/S1/objects/" + string(versionedUUID),
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.FetchObject(ctx, host, versionedClass, versionedShard, versionedUUID,
					nil, additional.Properties{}, 0)
				return err
			},
		},
		{
			name: "FetchObjects",
			path: "/replicas/indices/C1/shards/S1/objects",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.FetchObjects(ctx, host, versionedClass, versionedShard, []strfmt.UUID{versionedUUID})
				return err
			},
		},
		{
			name: "DigestObjects",
			path: "/replicas/indices/C1/shards/S1/objects/_digest",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.DigestObjects(ctx, host, versionedClass, versionedShard, []strfmt.UUID{versionedUUID}, 0)
				return err
			},
		},
		{
			name: "CountObjects",
			path: "/replicas/indices/C1/shards/S1/objects/_count",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.CountObjects(ctx, host, versionedClass, versionedShard)
				return err
			},
		},
		{
			name: "FindUUIDs",
			path: "/indices/C1/shards/S1/objects/_find",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.FindUUIDs(ctx, host, versionedClass, versionedShard, nil, 10)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			serv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tt.path, r.URL.Path)
				got = r.URL.Query().Get(replica.SchemaVersionKey)
				// Accepted, so the call does not climb the retry ladder.
				w.WriteHeader(http.StatusNoContent)
			}))
			defer serv.Close()

			client, err := NewReplicationClient(serv.Client())
			require.NoError(t, err)
			client.SetSchemaVersionProvider(func(string) uint64 { return 4711 })
			_ = tt.call(context.Background(), client, serv.URL[len("http://"):])

			assert.Equal(t, "4711", got, "read must carry %s", replica.SchemaVersionKey)
		})
	}
}
