//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitForStartup(t *testing.T) {
	t.Run("when common server is immediately ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{t: t})
		defer server.Close()
		v := New(server.URL, server.URL, nullLogger())
		err := v.WaitForStartup(context.Background(), 50*time.Millisecond)

		assert.Nil(t, err)
	})

	t.Run("when passage and query servers are immediately ready", func(t *testing.T) {
		serverPassage := httptest.NewServer(&testReadyHandler{t: t})
		serverQuery := httptest.NewServer(&testReadyHandler{t: t})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, nullLogger())
		err := v.WaitForStartup(context.Background(), 50*time.Millisecond)

		assert.Nil(t, err)
	})

	t.Run("when common server is down", func(t *testing.T) {
		url := "http://nothing-running-at-this-url"
		v := New(url, url, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err, nullLogger())
		assert.Contains(t, err.Error(), "init context expired before remote was ready: send check ready request")
	})

	t.Run("when passage and query servers are down", func(t *testing.T) {
		urlPassage := "http://nothing-running-at-this-url"
		urlQuery := "http://nothing-running-at-this-url-either"
		v := New(urlPassage, urlQuery, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err, nullLogger())
		assert.Contains(t, err.Error(), "init context expired before remote was ready")
		assert.Contains(t, err.Error(), "[passage] send check ready request")
		assert.Contains(t, err.Error(), "[query] send check ready request")
	})

	t.Run("when common server is alive, but not ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(1 * time.Minute),
		})
		defer server.Close()
		v := New(server.URL, server.URL, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "init context expired before remote was ready: send check ready request")
	})

	t.Run("when passage and query servers are alive, but not ready", func(t *testing.T) {
		rt := time.Now().Add(1 * time.Minute)
		serverPassage := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: rt,
		})
		serverQuery := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: rt,
		})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "init context expired before remote was ready")
		assert.Contains(t, err.Error(), "[passage] send check ready request")
		assert.Contains(t, err.Error(), "[query] send check ready request")
	})

	t.Run("when passage and query servers are alive, but query one is not ready", func(t *testing.T) {
		serverPassage := httptest.NewServer(&testReadyHandler{t: t})
		serverQuery := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(1 * time.Minute),
		})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "init context expired before remote was ready")
		assert.NotContains(t, err.Error(), "[passage] send check ready request")
		assert.Contains(t, err.Error(), "[query] send check ready request")
	})

	t.Run("when common server is initially not ready, but then becomes ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(100 * time.Millisecond),
		})
		v := New(server.URL, server.URL, nullLogger())
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.Nil(t, err)
	})

	t.Run("when passage and query servers are initially not ready, but then become ready", func(t *testing.T) {
		serverPassage := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(100 * time.Millisecond),
		})
		serverQuery := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(150 * time.Millisecond),
		})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.Nil(t, err)
	})
}

type testReadyHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	readyTime time.Time
}

func (f *testReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/.well-known/ready", r.URL.String())
	assert.Equal(f.t, http.MethodGet, r.Method)

	if time.Since(f.readyTime) < 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.WriteHeader(http.StatusNoContent)
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
