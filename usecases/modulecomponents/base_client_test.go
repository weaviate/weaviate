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

package modulecomponents

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBaseHttpClient(t *testing.T) {
	post := func(t *testing.T, client *http.Client, url string) (*http.Response, error) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url,
			bytes.NewReader([]byte(`{"text":"hello"}`)))
		require.NoError(t, err)
		return client.Do(req)
	}

	// dropConn ends the request without responding, which is what the client sees
	// when an inference service closes a connection it considers idle.
	dropConn := func(t *testing.T, w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		require.NoError(t, err)
		conn.Close()
	}

	// poolConn leaves a connection to the server in the client's idle pool, which
	// the next request reuses. Only a request sent on a pooled connection is
	// resent.
	poolConn := func(t *testing.T, client *http.Client, url string) {
		res, err := post(t, client, url)
		require.NoError(t, err)
		defer res.Body.Close()
		_, err = io.Copy(io.Discard, res.Body)
		require.NoError(t, err)
	}

	t.Run("retries a dropped connection", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) == 2 {
				dropConn(t, w, r)
				return
			}
			w.Write([]byte("second attempt"))
		}))
		defer server.Close()
		client := NewBaseHttpClient(30 * time.Second)
		poolConn(t, client, server.URL)

		res, err := post(t, client, server.URL)

		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, "second attempt", string(body))
		assert.Equal(t, int32(3), requests.Load())
	})

	t.Run("stops resending once the connection is freshly dialed", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if requests.Add(1) == 1 {
				w.Write([]byte("pooling the connection"))
				return
			}
			dropConn(t, w, r)
		}))
		defer server.Close()
		client := NewBaseHttpClient(30 * time.Second)
		poolConn(t, client, server.URL)

		res, err := post(t, client, server.URL)
		if res != nil {
			defer res.Body.Close()
		}

		require.Error(t, err)
		assert.Contains(t, err.Error(), "EOF")
		assert.Equal(t, int32(3), requests.Load())
	})

	// blockingServer never finishes its response, so only the client's own
	// timeout can end the request. The release channel closes before
	// server.Close, which would otherwise block on the handler.
	blockingServer := func(t *testing.T, respond func(w http.ResponseWriter)) *httptest.Server {
		release := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if respond != nil {
				respond(w)
			}
			<-release
		}))
		t.Cleanup(server.Close)
		t.Cleanup(func() { close(release) })
		return server
	}

	t.Run("reports the timeout as a deadline", func(t *testing.T) {
		server := blockingServer(t, nil)
		client := NewBaseHttpClient(200 * time.Millisecond)

		// On http.Client the timeout would report a cancellation, not a deadline.
		require.Zero(t, client.Timeout)

		res, err := post(t, client, server.URL)
		if res != nil {
			defer res.Body.Close()
		}

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
		assert.NotContains(t, err.Error(), "request canceled")
	})

	t.Run("keeps the dial guard in place", func(t *testing.T) {
		t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer server.Close()

		res, err := post(t, NewBaseHttpClient(30*time.Second), server.URL)
		if res != nil {
			defer res.Body.Close()
		}

		require.Error(t, err)
		assert.Contains(t, err.Error(), "refusing to dial internal address")
	})

	t.Run("keeps the timeout in force while the body is read", func(t *testing.T) {
		server := blockingServer(t, func(w http.ResponseWriter) {
			w.Write([]byte("partial"))
			w.(http.Flusher).Flush()
		})

		res, err := post(t, NewBaseHttpClient(200*time.Millisecond), server.URL)
		require.NoError(t, err)
		defer res.Body.Close()

		_, err = io.ReadAll(res.Body)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
		assert.NotContains(t, err.Error(), "request canceled")
	})
}

func TestNewBaseHttpClientWithIdleConns(t *testing.T) {
	get := func(t *testing.T, client *http.Client, url string) error {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		require.NoError(t, err)
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		_, err = io.Copy(io.Discard, res.Body)
		return err
	}

	t.Run("keeps a connection per concurrent request between rounds", func(t *testing.T) {
		var opened atomic.Int32
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(20 * time.Millisecond)
		}))
		server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				opened.Add(1)
			}
		}
		server.Start()
		defer server.Close()
		client := NewBaseHttpClientWithIdleConns(30*time.Second, 8)

		round := func() {
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					assert.NoError(t, get(t, client, server.URL))
				})
			}
			wg.Wait()
		}
		round()
		first := opened.Load()
		require.Greater(t, first, int32(2))
		round()

		assert.Equal(t, first, opened.Load())
	})

	t.Run("leaves the shared default transport as it is", func(t *testing.T) {
		shared := http.DefaultTransport.(*http.Transport)
		perHost, total := shared.MaxIdleConnsPerHost, shared.MaxIdleConns

		NewBaseHttpClientWithIdleConns(30*time.Second, 64)

		assert.Equal(t, perHost, shared.MaxIdleConnsPerHost)
		assert.Equal(t, total, shared.MaxIdleConns)
	})

	t.Run("keeps the dial guard in place", func(t *testing.T) {
		t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer server.Close()

		err := get(t, NewBaseHttpClientWithIdleConns(30*time.Second, 8), server.URL)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "refusing to dial internal address")
	})
}
