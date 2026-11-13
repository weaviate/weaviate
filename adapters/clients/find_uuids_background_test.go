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
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	clusterapi "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/backgroundaccess"
)

func TestFindUUIDsCarriesTheBackgroundMark(t *testing.T) {
	type findFn func(ctx context.Context, host string) ([]strfmt.UUID, error)
	clientsUnderTest := []struct {
		name string
		find func(t *testing.T, hc *http.Client) findFn
	}{
		{name: "remote index", find: func(t *testing.T, hc *http.Client) findFn {
			c := newRemoteIndex(hc)
			return func(ctx context.Context, host string) ([]strfmt.UUID, error) {
				return c.FindUUIDs(ctx, host, "C1", "S1", nil, 10)
			}
		}},
		{name: "replication", find: func(t *testing.T, hc *http.Client) findFn {
			c := newReplicationClient(t, hc)
			return func(ctx context.Context, host string) ([]strfmt.UUID, error) {
				return c.FindUUIDs(ctx, host, "C1", "S1", nil, 10)
			}
		}},
	}
	marks := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{name: "request", ctx: context.Background(), want: ""},
		{name: "background", ctx: backgroundaccess.With(context.Background()), want: "true"},
	}
	for _, cut := range clientsUnderTest {
		for _, m := range marks {
			t.Run(cut.name+"/"+m.name, func(t *testing.T) {
				var got atomic.Pointer[string]
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					h := r.Header.Get(backgroundaccess.Header)
					got.Store(&h)
					clusterapi.IndicesPayloads.FindUUIDsResults.SetContentTypeHeader(w)
					b, err := clusterapi.IndicesPayloads.FindUUIDsResults.Marshal([]strfmt.UUID{"a"})
					require.NoError(t, err)
					_, err = w.Write(b)
					require.NoError(t, err)
				}))
				defer ts.Close()

				uuids, err := cut.find(t, ts.Client())(m.ctx, ts.URL[len("http://"):])
				require.NoError(t, err)
				require.Equal(t, []strfmt.UUID{"a"}, uuids)
				require.NotNil(t, got.Load())
				require.Equal(t, m.want, *got.Load())
			})
		}
	}
}
