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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/backup"
)

// staticResolver maps node names to the httptest hosts a test spun up.
type staticResolver map[string]string

func (r staticResolver) NodeHostname(nodeName string) (string, bool) {
	host, ok := r[nodeName]
	return host, ok
}

func TestClusterBackupActivity(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		// notFound answers the way a node's catch-all handler does, which is
		// the only 404 the client reads as "older build".
		notFound   bool
		want       backup.NodeActivity
		wantErr    error
		wantErrMsg []string
	}{
		{
			name:       "busy",
			statusCode: http.StatusOK,
			body:       `{"busy":true,"kind":"restore","id":"restore-7"}`,
			want:       backup.NodeActivity{Busy: true, Kind: "restore", ID: "restore-7"},
		},
		{
			name:       "idle",
			statusCode: http.StatusOK,
			body:       `{"busy":false}`,
			want:       backup.NodeActivity{},
		},
		{
			name:     "route not served",
			notFound: true,
			wantErr:  ErrNodeActivityUnsupported,
		},
		{
			name:       "server error",
			statusCode: http.StatusInternalServerError,
			body:       "probe exploded",
			wantErrMsg: []string{"500", "probe exploded"},
		},
		{
			name:       "malformed payload",
			statusCode: http.StatusOK,
			body:       "not json",
			wantErrMsg: []string{"unmarshal node activity response"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, pathBackupNodeActivity, r.URL.Path)
				if tt.notFound {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			c := NewClusterBackupActivity(server.Client(), resolverFor(t, "node1", server.URL))
			got, err := c.NodeActivity(context.Background(), "node1")

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case len(tt.wantErrMsg) > 0:
				require.Error(t, err)
				for _, want := range tt.wantErrMsg {
					assert.Contains(t, err.Error(), want)
				}
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}

	t.Run("unresolvable node", func(t *testing.T) {
		c := NewClusterBackupActivity(http.DefaultClient, staticResolver{})
		_, err := c.NodeActivity(context.Background(), "ghost")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unable to resolve hostname")
	})

	t.Run("unreachable node", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		resolver := resolverFor(t, "node1", server.URL)
		client := server.Client()
		server.Close()

		c := NewClusterBackupActivity(client, resolver)
		_, err := c.NodeActivity(context.Background(), "node1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "node activity request")
	})

	t.Run("canceled context", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		c := NewClusterBackupActivity(server.Client(), resolverFor(t, "node1", server.URL))
		_, err := c.NodeActivity(ctx, "node1")
		require.ErrorIs(t, err, context.Canceled)
	})
}

func resolverFor(t *testing.T, nodeName, serverURL string) staticResolver {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	require.NoError(t, err)
	return staticResolver{nodeName: parsed.Host}
}
