//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
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
		v := New(server.URL, server.URL, 0, nullLogger())
		err := v.WaitForStartup(context.Background(), 150*time.Millisecond)

		assert.Nil(t, err)
	})

	t.Run("when passage and query servers are immediately ready", func(t *testing.T) {
		serverPassage := httptest.NewServer(&testReadyHandler{t: t})
		serverQuery := httptest.NewServer(&testReadyHandler{t: t})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, 0, nullLogger())
		err := v.WaitForStartup(context.Background(), 150*time.Millisecond)

		assert.Nil(t, err)
	})

	t.Run("when common server is down", func(t *testing.T) {
		url := "http://nothing-running-at-this-url"
		v := New(url, url, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err, nullLogger())
		assert.Contains(t, err.Error(), "init context expired before remote was ready: send check ready request")
		assertContainsEither(t, err.Error(), "dial tcp", "context deadline exceeded")
		assert.NotContains(t, err.Error(), "[passage]")
		assert.NotContains(t, err.Error(), "[query]")
	})

	t.Run("when passage and query servers are down", func(t *testing.T) {
		urlPassage := "http://nothing-running-at-this-url"
		urlQuery := "http://nothing-running-at-this-url-either"
		v := New(urlPassage, urlQuery, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err, nullLogger())
		assert.Contains(t, err.Error(), "[passage] init context expired before remote was ready: send check ready request")
		assert.Contains(t, err.Error(), "[query] init context expired before remote was ready: send check ready request")
		assertContainsEither(t, err.Error(), "dial tcp", "context deadline exceeded")
	})

	t.Run("when common server is alive, but not ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(time.Hour),
		})
		defer server.Close()
		v := New(server.URL, server.URL, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "init context expired before remote was ready")
		assertContainsEither(t, err.Error(), "not ready: status 503", "context deadline exceeded")
		assert.NotContains(t, err.Error(), "[passage]")
		assert.NotContains(t, err.Error(), "[query]")
	})

	t.Run("when passage and query servers are alive, but not ready", func(t *testing.T) {
		rt := time.Now().Add(time.Hour)
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
		v := New(serverPassage.URL, serverQuery.URL, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "[passage] init context expired before remote was ready")
		assert.Contains(t, err.Error(), "[query] init context expired before remote was ready")
		assertContainsEither(t, err.Error(), "not ready: status 503", "context deadline exceeded")
	})

	t.Run("when passage and query servers are alive, but query one is not ready", func(t *testing.T) {
		serverPassage := httptest.NewServer(&testReadyHandler{t: t})
		serverQuery := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(1 * time.Minute),
		})
		defer serverPassage.Close()
		defer serverQuery.Close()
		v := New(serverPassage.URL, serverQuery.URL, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "[query] init context expired before remote was ready")
		assertContainsEither(t, err.Error(), "not ready: status 503", "context deadline exceeded")
		assert.NotContains(t, err.Error(), "[passage]")
	})

	t.Run("when common server is initially not ready, but then becomes ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(100 * time.Millisecond),
		})
		v := New(server.URL, server.URL, 0, nullLogger())
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
		v := New(serverPassage.URL, serverQuery.URL, 0, nullLogger())
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
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func assertContainsEither(t *testing.T, str string, contains ...string) {
	reg := regexp.MustCompile(strings.Join(contains, "|"))
	assert.Regexp(t, reg, str)
}
