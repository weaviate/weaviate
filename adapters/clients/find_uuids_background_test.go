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
	"github.com/stretchr/testify/require"

	clusterapi "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/backgroundaccess"
)

func TestFindUUIDsCarriesTheBackgroundMark(t *testing.T) {
	headers := make(chan string, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers <- r.Header.Get(backgroundaccess.Header)
		clusterapi.IndicesPayloads.FindUUIDsResults.SetContentTypeHeader(w)
		b, err := clusterapi.IndicesPayloads.FindUUIDsResults.Marshal([]strfmt.UUID{"a"})
		require.NoError(t, err)
		_, err = w.Write(b)
		require.NoError(t, err)
	}))
	defer ts.Close()
	host := ts.URL[len("http://"):]
	remote, replication := newRemoteIndex(ts.Client()), newReplicationClient(t, ts.Client())

	for _, tt := range []struct {
		ctx  context.Context
		want string
	}{
		{ctx: context.Background()},
		{ctx: backgroundaccess.With(context.Background()), want: "true"},
	} {
		uuids, err := remote.FindUUIDs(tt.ctx, host, "C1", "S1", nil, 10)
		require.NoError(t, err)
		require.Equal(t, []strfmt.UUID{"a"}, uuids)
		require.Equal(t, tt.want, <-headers, "remote index")
		uuids, err = replication.FindUUIDs(tt.ctx, host, "C1", "S1", nil, 10)
		require.NoError(t, err)
		require.Equal(t, []strfmt.UUID{"a"}, uuids)
		require.Equal(t, tt.want, <-headers, "replication client")
	}
}
