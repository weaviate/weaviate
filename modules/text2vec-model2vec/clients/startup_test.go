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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitForStartup(t *testing.T) {
	t.Run("when common server is immediately ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{t: t})
		defer server.Close()
		v := New(server.URL, 0, nullLogger())
		err := v.WaitForStartup(context.Background(), 150*time.Millisecond)

		assert.Nil(t, err)
	})

	t.Run("when common server is down", func(t *testing.T) {
		url := "http://nothing-running-at-this-url"
		v := New(url, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err, nullLogger())
		assert.Contains(t, err.Error(), "init context expired before remote was ready: send check ready request")
		assertContainsEither(t, err.Error(), "dial tcp", "context deadline exceeded")
		assert.NotContains(t, err.Error(), "[passage]")
		assert.NotContains(t, err.Error(), "[query]")
	})

	t.Run("when common server is alive, but not ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(time.Hour),
		})
		defer server.Close()
		v := New(server.URL, 0, nullLogger())
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err := v.WaitForStartup(ctx, 50*time.Millisecond)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "init context expired before remote was ready")
		assertContainsEither(t, err.Error(), "not ready: status 503", "context deadline exceeded")
		assert.NotContains(t, err.Error(), "[passage]")
		assert.NotContains(t, err.Error(), "[query]")
	})

	t.Run("when common server is initially not ready, but then becomes ready", func(t *testing.T) {
		server := httptest.NewServer(&testReadyHandler{
			t:         t,
			readyTime: time.Now().Add(100 * time.Millisecond),
		})
		v := New(server.URL, 0, nullLogger())
		defer server.Close()
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

func assertContainsEither(t *testing.T, str string, contains ...string) {
	reg := regexp.MustCompile(strings.Join(contains, "|"))
	assert.Regexp(t, reg, str)
}
