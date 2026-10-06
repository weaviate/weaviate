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
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestRemoteIndexReadsCarrySchemaVersion checks every remote read puts the version on the wire.
// Without it the receiving node has to answer "not ready" to every miss.
func TestRemoteIndexReadsCarrySchemaVersion(t *testing.T) {
	const (
		version  = "4711"
		objectID = strfmt.UUID("8c29da7a-600a-43b4-ab0f-9ba6e2e3d9b3")
	)

	tests := []struct {
		name string
		path string
		// respond writes a reply the client accepts, so the call does not retry.
		respond func(w http.ResponseWriter)
		call    func(ctx context.Context, c *RemoteIndex, host string) error
	}{
		{
			name:    "GetObject",
			path:    "/indices/C1/shards/S1/objects/" + string(objectID),
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.GetObject(ctx, host, "C1", "S1", objectID,
					search.SelectProperties{}, additional.Properties{}, 4711)
				return err
			},
		},
		{
			name:    "Exists",
			path:    "/indices/C1/shards/S1/objects/" + string(objectID),
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.Exists(ctx, host, "C1", "S1", objectID, 4711)
				return err
			},
		},
		{
			name:    "MultiGetObjects",
			path:    "/indices/C1/shards/S1/objects",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.MultiGetObjects(ctx, host, "C1", "S1", nil, 4711)
				return err
			},
		},
		{
			name:    "SearchShard",
			path:    "/indices/C1/shards/S1/objects/_search",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, _, _, err := c.SearchShard(ctx, host, "C1", "S1",
					nil, nil, 0, 10, nil, nil, nil, nil, nil, additional.Properties{}, nil, nil, 4711)
				return err
			},
		},
		{
			name:    "Aggregate",
			path:    "/indices/C1/shards/S1/objects/_aggregations",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.Aggregate(ctx, host, "C1", "S1", aggregation.Params{}, 4711)
				return err
			},
		},
		{
			name:    "FindUUIDs",
			path:    "/indices/C1/shards/S1/objects/_find",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.FindUUIDs(ctx, host, "C1", "S1", nil, 10, 4711)
				return err
			},
		},
		{
			name:    "GetShardQueueSize",
			path:    "/indices/C1/shards/S1/queuesize",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.GetShardQueueSize(ctx, host, "C1", "S1", 4711)
				return err
			},
		},
		{
			name:    "GetShardStatus",
			path:    "/indices/C1/shards/S1/status",
			respond: func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) },
			call: func(ctx context.Context, c *RemoteIndex, host string) error {
				_, err := c.GetShardStatus(ctx, host, "C1", "S1", 4711)
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
			// Keep a non-2xx reply from stretching into the retry ladder.
			client.timeoutUnit = 20 * time.Millisecond
			_ = tt.call(context.Background(), client, serv.URL[7:])

			assert.Equal(t, version, got, "read must carry %s", replica.SchemaVersionKey)
		})
	}
}
