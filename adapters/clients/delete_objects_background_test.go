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
	"github.com/stretchr/testify/require"

	clusterapi "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/backgroundaccess"
)

func TestDeleteObjectsCarriesTheBackgroundMark(t *testing.T) {
	headers := make(chan string, 1)
	remoteTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get(backgroundaccess.Header)
		clusterapi.IndicesPayloads.BatchDeleteResults.SetContentTypeHeader(w)
		_, err := w.Write([]byte("[]"))
		require.NoError(t, err)
	}))
	defer remoteTS.Close()
	replicaTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get(backgroundaccess.Header)
		_, err := w.Write([]byte("{}"))
		require.NoError(t, err)
	}))
	defer replicaTS.Close()
	remote, replication := newRemoteIndex(remoteTS.Client()), newReplicationClient(t, replicaTS.Client())
	uuids, deletionTime := []strfmt.UUID{"a"}, time.UnixMilli(1)

	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{name: "user request", ctx: context.Background()},
		{name: "background work", ctx: backgroundaccess.With(context.Background()), want: "true"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := remote.DeleteObjectBatch(tt.ctx, remoteTS.URL[len("http://"):], "C1", "S1", uuids, deletionTime, false, 1)
			require.Empty(t, res)
			require.Equal(t, tt.want, <-headers, "remote index")

			_, err := replication.DeleteObjects(tt.ctx, replicaTS.URL[len("http://"):], "C1", "S1", "r1", uuids, deletionTime, false, 1)
			require.NoError(t, err)
			require.Equal(t, tt.want, <-headers, "replication client")
		})
	}
}
