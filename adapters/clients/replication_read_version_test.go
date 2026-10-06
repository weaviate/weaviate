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

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestReplicationReadsCarrySchemaVersion checks every replicated read puts the version on the
// wire. FetchObjects is the one worth pinning: it rebuilds RawQuery for its id list, which
// silently dropped what the request builder had already put there.
func TestReplicationReadsCarrySchemaVersion(t *testing.T) {
	const (
		version  = "4711"
		objectID = strfmt.UUID("8c29da7a-600a-43b4-ab0f-9ba6e2e3d9b3")
	)

	tests := []struct {
		name string
		path string
		call func(ctx context.Context, c *replicationClient, host string) error
	}{
		{
			name: "FetchObject",
			path: "/replicas/indices/C1/shards/S1/objects/" + string(objectID),
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.FetchObject(ctx, host, "C1", "S1", objectID, nil, additional.Properties{}, 0, 4711)
				return err
			},
		},
		{
			name: "FetchObjects",
			path: "/replicas/indices/C1/shards/S1/objects",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.FetchObjects(ctx, host, "C1", "S1", []strfmt.UUID{objectID}, 4711)
				return err
			},
		},
		{
			name: "DigestObjects",
			path: "/replicas/indices/C1/shards/S1/objects/_digest",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.DigestObjects(ctx, host, "C1", "S1", []strfmt.UUID{objectID}, 0, 4711)
				return err
			},
		},
		{
			name: "CountObjects",
			path: "/replicas/indices/C1/shards/S1/objects/_count",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.CountObjects(ctx, host, "C1", "S1", 4711)
				return err
			},
		},
		{
			name: "FindUUIDs",
			path: "/indices/C1/shards/S1/objects/_find",
			call: func(ctx context.Context, c *replicationClient, host string) error {
				_, err := c.FindUUIDs(ctx, host, "C1", "S1", nil, 10, 4711)
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

			client := newReplicationClient(t, serv.Client())
			_ = tt.call(context.Background(), client, serv.URL[len("http://"):])

			assert.Equal(t, version, got, "read must carry %s", replica.SchemaVersionKey)
		})
	}
}
