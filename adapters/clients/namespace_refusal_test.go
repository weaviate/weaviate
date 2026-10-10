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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestRebuildHTTPRefusal(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		refusal error
		rebuilt bool
	}{
		{
			name:    "a 500 ending in a refusal's text is not rebuilt",
			code:    http.StatusInternalServerError,
			refusal: namespaces.ErrNamespaceSuspended,
		},
		{
			name:    "a 503 ending in the resuming text is rebuilt",
			code:    http.StatusServiceUnavailable,
			refusal: namespaces.ErrNamespaceResuming,
			rebuilt: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := &HTTPError{Code: tc.code, Body: []byte(`get shard "s": ` + tc.refusal.Error() + "\n")}

			err := rebuildHTTPRefusal(in, tc.code, in.Body)

			assert.Equal(t, tc.rebuilt, errors.Is(err, tc.refusal))
			assert.Equal(t, in.Error(), err.Error())
			he, ok := AsHTTPError(err)
			require.True(t, ok)
			assert.Same(t, in, he)
		})
	}
}

func TestClientsRebuildNamespaceRefusal(t *testing.T) {
	tests := []struct {
		name string
		call func(t *testing.T, c *http.Client, host string) error
	}{
		{
			name: "RemoteIndex.Exists",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newRemoteIndex(c).Exists(context.Background(), host, "C1", "S1", UUID1)
				return err
			},
		},
		{
			name: "RemoteIndex.BatchAddReferences",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).BatchAddReferences(context.Background(), host, "C1", "S1", objects.BatchReferences{{}}, 0)[0]
			},
		},
		{
			name: "RemoteIndex.GetObject",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newRemoteIndex(c).GetObject(context.Background(), host, "C1", "S1", UUID1, nil, additional.Properties{})
				return err
			},
		},
		{
			name: "RemoteIndex.DeleteObject",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).DeleteObject(context.Background(), host, "C1", "S1", UUID1, time.Now(), 0)
			},
		},
		{
			name: "RemoteIndex.MergeObject",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).MergeObject(context.Background(), host, "C1", "S1", objects.MergeDocument{ID: UUID1}, 0)
			},
		},
		{
			name: "RemoteIndex.MultiGetObjects",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newRemoteIndex(c).MultiGetObjects(context.Background(), host, "C1", "S1", []strfmt.UUID{UUID1})
				return err
			},
		},
		{
			name: "RemoteIndex.FindUUIDs",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newRemoteIndex(c).FindUUIDs(context.Background(), host, "C1", "S1", nil, 10)
				return err
			},
		},
		{
			name: "RemoteIndex.DeleteObjectBatch",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).DeleteObjectBatch(context.Background(), host, "C1", "S1", []strfmt.UUID{UUID1}, time.Now(), false, 0)[0].Err
			},
		},
		{
			name: "RemoteIndex.GetShardQueueSize",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newRemoteIndex(c).GetShardQueueSize(context.Background(), host, "C1", "S1")
				return err
			},
		},
		{
			name: "RemoteIndex.GetShardStatus",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newRemoteIndex(c).GetShardStatus(context.Background(), host, "C1", "S1")
				return err
			},
		},
		{
			name: "RemoteIndex.UpdateShardStatus",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).UpdateShardStatus(context.Background(), host, "C1", "S1", "READONLY", 0)
			},
		},
		{
			name: "RemoteIndex.PutFile",
			call: func(t *testing.T, c *http.Client, host string) error {
				payload := struct {
					*strings.Reader
					io.Closer
				}{strings.NewReader("hello, world"), io.NopCloser(nil)}
				return newRemoteIndex(c).PutFile(context.Background(), host, "C1", "S1", "file1", payload)
			},
		},
		{
			name: "RemoteIndex.CreateShard",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).CreateShard(context.Background(), host, "C1", "S1")
			},
		},
		{
			name: "RemoteIndex.ReInitShard",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).ReInitShard(context.Background(), host, "C1", "S1")
			},
		},
		{
			name: "RemoteIndex.AddAsyncReplicationTargetNode",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).AddAsyncReplicationTargetNode(context.Background(), host, "C1", "S1",
					additional.AsyncReplicationTargetNodeOverride{}, 0)
			},
		},
		{
			name: "RemoteIndex.RemoveAsyncReplicationTargetNode",
			call: func(t *testing.T, c *http.Client, host string) error {
				return newRemoteIndex(c).RemoveAsyncReplicationTargetNode(context.Background(), host, "C1", "S1",
					additional.AsyncReplicationTargetNodeOverride{})
			},
		},
		{
			name: "replicationClient.CountObjects",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newReplicationClient(t, c).CountObjects(context.Background(), host, "C1", "S1")
				return err
			},
		},
		{
			name: "replicationClient.FindUUIDs",
			call: func(t *testing.T, c *http.Client, host string) error {
				_, err := newReplicationClient(t, c).FindUUIDs(context.Background(), host, "C1", "S1", nil, 10)
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, `get shard "S1": `+namespaces.ErrNamespaceSuspended.Error(), http.StatusUnprocessableEntity)
			}))
			defer server.Close()

			err := tc.call(t, server.Client(), server.Listener.Addr().String())

			require.ErrorIs(t, err, namespaces.ErrNamespaceSuspended)
		})
	}
}
