package network

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
)

func TestProxyGetInstance(t *testing.T) {
	var (
		subject *network
		remote  *httptest.Server
	)

	arrange := func(matchers ...requestMatcher) {
		remote = fakeRemoteInstanceWithGraphQL(t, matchers...)
		subject = &network{
			peers: []Peer{{
				Name: "best-instance",
				URI:  strfmt.URI(remote.URL),
				Id:   strfmt.UUID("some-id"),
			}},
		}
	}

	act := func() {
		subject.ProxyGetInstance(ProxyGetInstanceParams{
			SubQuery:       []byte("foo"),
			TargetInstance: "best-instance",
		})
	}

	cleanUp := func() {
		remote.Close()
	}

	t.Run("handler should be called", func(t *testing.T) {
		called := false
		matcher := func(t *testing.T, r *http.Request) {
			called = true
		}
		arrange(matcher)
		act()

		if called == false {
			t.Error("handler was never called")
		}

		cleanUp()
	})

	t.Run("should be post request", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method to be POST, but got %s", r.Method)
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})

	t.Run("should call correct url path", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			expectedPath := "/weaviate/v1/graphql"
			if r.URL.Path != expectedPath {
				t.Errorf("expected path to be %s, but was %s", expectedPath, r.URL.Path)
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})
}

type requestMatcher func(t *testing.T, r *http.Request)

func fakeRemoteInstanceWithGraphQL(t *testing.T, matchers ...requestMatcher) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, matcher := range matchers {
			matcher(t, r)
		}
		fmt.Fprintln(w, "Hello, client")
	}))
	return ts
}
