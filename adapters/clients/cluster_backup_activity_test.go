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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type staticResolver map[string]string

func (r staticResolver) NodeHostname(nodeName string) (string, bool) {
	host, ok := r[nodeName]
	return host, ok
}

const idleAnswer = `{"probe":"weaviate/backup-node-activity","busy":false}`

// padTo pads valid JSON with trailing whitespace, which decoders accept, so an
// answer that is only wrong about its size is refused for its size alone.
func padTo(size int) string {
	return idleAnswer + strings.Repeat(" ", size-len(idleAnswer))
}

func nosniff(w http.ResponseWriter, status int, body string) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	w.Write([]byte(body))
}

func TestClusterBackupActivityWireContract(t *testing.T) {
	tests := []struct {
		name string
		// unsupported marks the answers that are terminal, i.e. that let the
		// caller stop asking and treat the node as too old to have one.
		unsupported  bool
		unauthorized bool
		respond      func(w http.ResponseWriter, r *http.Request)
		want         backup.NodeActivity
		wantErr      string
	}{
		{
			name: "busy with a backup",
			respond: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"probe":"weaviate/backup-node-activity","busy":true,"kind":"backup","id":"b1"}`))
			},
			want: backup.NodeActivity{Busy: true, Kind: "backup", ID: "b1"},
		},
		{
			name: "busy with a restore",
			respond: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"probe":"weaviate/backup-node-activity","busy":true,"kind":"restore","id":"r1"}`))
			},
			want: backup.NodeActivity{Busy: true, Kind: "restore", ID: "r1"},
		},
		{
			name:    "idle",
			respond: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(idleAnswer)) },
			want:    backup.NodeActivity{},
		},
		{
			name:    "200 carrying another service's marker",
			respond: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"probe":"nginx","busy":false}`)) },
			wantErr: "was not written by the node-activity route",
		},
		{
			name: "200 that never mentions busy",
			respond: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"probe":"weaviate/backup-node-activity"}`))
			},
			wantErr: `answer has no "busy" field`,
		},
		{
			name:    "200 that is not JSON",
			respond: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`<html>hello</html>`)) },
			wantErr: "unmarshal node activity answer",
		},
		{
			name:        "the node's own 404",
			respond:     func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
			unsupported: true,
		},
		{
			name: "404 with the node's body but no nosniff",
			respond: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("404 page not found\n"))
			},
			wantErr: "did not come from the node itself",
		},
		{
			name:    "404 with nosniff but a proxy's body",
			respond: func(w http.ResponseWriter, r *http.Request) { nosniff(w, http.StatusNotFound, "<html>nginx</html>") },
			wantErr: "did not come from the node itself",
		},
		{
			name: "proxy-shaped 404",
			respond: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html>404 Not Found</html>"))
			},
			wantErr: "did not come from the node itself",
		},
		{
			name: "plain 503",
			respond: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "overloaded", http.StatusServiceUnavailable)
			},
			wantErr: "status 503",
		},
		{
			name: "503 carrying a not-wired sentinel",
			respond: func(w http.ResponseWriter, r *http.Request) {
				nosniff(w, http.StatusServiceUnavailable, "weaviate/probe-not-wired")
			},
			wantErr: "status 503",
		},
		{
			name:         "401 with an empty body",
			respond:      func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) },
			unauthorized: true,
			wantErr:      "CLUSTER_BASIC_AUTH_USERNAME",
		},
		{
			name:         "403 with an empty body",
			respond:      func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) },
			unauthorized: true,
			wantErr:      "CLUSTER_BASIC_AUTH_USERNAME",
		},
		{
			name:    "an answer exactly at the size cap",
			respond: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(padTo(64 << 10))) },
			want:    backup.NodeActivity{},
		},
		{
			name:    "an answer one byte over the size cap",
			respond: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(padTo(64<<10 + 1))) },
			wantErr: "answer exceeds 65536 bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod, gotPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath = r.Method, r.URL.Path
				tt.respond(w, r)
			}))
			defer server.Close()

			client := newTestBackupActivity(t, server)
			got, err := client.NodeActivity(context.Background(), "node1")

			assert.Equal(t, http.MethodGet, gotMethod)
			assert.Equal(t, "/backups/node-activity", gotPath)
			switch {
			case tt.unsupported:
				require.ErrorIs(t, err, ErrNodeActivityUnsupported)
			case tt.wantErr != "":
				require.ErrorContains(t, err, tt.wantErr)
				require.NotErrorIs(t, err, ErrNodeActivityUnsupported,
					"only the node's own 404 may let the caller stop asking")
				assert.Equal(t, tt.unauthorized, errors.Is(err, ErrProbeUnauthorized))
			default:
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got, "no refused answer may read as a free node")
		})
	}
}

func TestClusterBackupActivityUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	client := newTestBackupActivity(t, server)
	server.Close()

	tests := []struct {
		name    string
		node    string
		wantErr string
	}{
		{name: "node is not a cluster member", node: "node2", wantErr: "cannot resolve hostname"},
		{name: "node does not answer", node: "node1", wantErr: "node activity request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.NodeActivity(context.Background(), tt.node)

			require.ErrorContains(t, err, tt.wantErr)
			require.NotErrorIs(t, err, ErrNodeActivityUnsupported)
			assert.Equal(t, backup.NodeActivity{}, got)
		})
	}
}

// The probe must not go through HTTP_PROXY/HTTPS_PROXY: a proxy answering in a
// peer's stead 404s, and the caller reads a 404 as "too old to ask", so one
// proxy would report every node in the cluster free of backups. And no
// configured budget may leave the caller unbounded.
func TestProbeHTTPClientIgnoresProxiesAndAlwaysBounds(t *testing.T) {
	withAuth := cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "u", Password: "p"}}

	tests := []struct {
		name    string
		auth    cluster.AuthConfig
		timeout time.Duration
	}{
		{name: "without basic auth", timeout: time.Second},
		{name: "with basic auth", auth: withAuth, timeout: time.Second},
		{name: "with a zero budget", timeout: 0},
		{name: "with a negative budget", timeout: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := probeHTTPClient(tt.auth, tt.timeout)

			transport := client.Transport
			if authed, ok := transport.(basicAuthTransport); ok {
				transport = authed.next
			}
			require.IsType(t, &http.Transport{}, transport)
			assert.Nil(t, transport.(*http.Transport).Proxy)
			assert.Positive(t, client.Timeout, "no configured budget may leave a probe unbounded")
		})
	}
}

// A peer that answers 3xx names the host that answers in its stead. The probe
// must not ask it: a valid marker from a host we never addressed reads as a node
// with nothing running, and reaching it at all is what hands over the credential.
func TestClusterBackupActivityRefusesRedirects(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		elsewhere func(w http.ResponseWriter, r *http.Request)
	}{
		{
			name:      "302 to a host answering idle",
			status:    http.StatusFound,
			elsewhere: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(idleAnswer)) },
		},
		{
			name:      "302 to a host answering a stock 404",
			status:    http.StatusFound,
			elsewhere: func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
		},
		{
			name:      "307 to a host answering idle",
			status:    http.StatusTemporaryRedirect,
			elsewhere: func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(idleAnswer)) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := make(chan string, 4)
			elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seen <- r.Header.Get("Authorization")
				tt.elsewhere(w, r)
			}))
			defer elsewhere.Close()
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, elsewhere.URL+r.URL.Path, tt.status)
			}))
			defer peer.Close()

			auth := cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "u", Password: "p"}}
			client := NewClusterBackupActivity(auth, time.Second,
				staticResolver{"node1": strings.TrimPrefix(peer.URL, "http://")})

			got, err := client.NodeActivity(context.Background(), "node1")

			assert.Zero(t, len(seen), "a request never made cannot hand over the credential")
			assert.Equal(t, backup.NodeActivity{}, got)
			assert.NotErrorIs(t, err, ErrNodeActivityUnsupported)
			require.Error(t, err)
			assert.ErrorContains(t, err, "does not follow")
			assert.ErrorContains(t, err, elsewhere.URL)
		})
	}
}

// A caller may have no deadline of its own, so the probe carries the bound: a
// peer that accepts the connection and then stalls must not hold it forever.
func TestClusterBackupActivityBoundsAStalledPeer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	client := NewClusterBackupActivity(cluster.AuthConfig{}, 50*time.Millisecond,
		staticResolver{"node1": strings.TrimPrefix(server.URL, "http://")})

	done := make(chan error, 1)
	go func() {
		_, err := client.NodeActivity(context.Background(), "node1")
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrNodeActivityUnsupported)
	case <-time.After(2 * time.Second):
		t.Fatal("a probe with no caller deadline never returned")
	}
}

type recordingTransport struct{ got *http.Request }

func (t *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.got = r
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
}

// An http.RoundTripper may not edit the request it is given: the stdlib reuses
// that request across a redirect, and the credential it added would travel with
// it to whatever host the redirect named.
func TestBasicAuthTransportDoesNotEditTheCallersRequest(t *testing.T) {
	inner := &recordingTransport{}
	transport := basicAuthTransport{next: inner, auth: cluster.BasicAuth{Username: "u", Password: "p"}}
	req, err := http.NewRequest(http.MethodGet, "http://peer/backups/node-activity", nil)
	require.NoError(t, err)

	res, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer res.Body.Close()

	assert.Empty(t, req.Header.Get("Authorization"), "the caller's request must come back untouched")
	user, password, ok := inner.got.BasicAuth()
	require.True(t, ok)
	assert.Equal(t, "u", user)
	assert.Equal(t, "p", password)
}

// The route sits behind the cluster's basic auth, so the probe has to present
// the cluster's own credentials to read a peer at all.
func TestClusterBackupActivitySendsBasicAuth(t *testing.T) {
	auth := cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "cluster-user", Password: "s3cret"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != auth.BasicAuth.Username || password != auth.BasicAuth.Password {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(idleAnswer))
	}))
	defer server.Close()
	client := NewClusterBackupActivity(auth, time.Second,
		staticResolver{"node1": strings.TrimPrefix(server.URL, "http://")})

	got, err := client.NodeActivity(context.Background(), "node1")

	require.NoError(t, err)
	assert.Equal(t, backup.NodeActivity{}, got)
}

func newTestBackupActivity(t *testing.T, server *httptest.Server) *ClusterBackupActivity {
	t.Helper()
	return NewClusterBackupActivity(cluster.AuthConfig{}, time.Second,
		staticResolver{"node1": strings.TrimPrefix(server.URL, "http://")})
}
