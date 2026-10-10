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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/namespaces"
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
