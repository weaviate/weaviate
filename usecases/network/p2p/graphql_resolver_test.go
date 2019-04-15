/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package p2p

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/network/common"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/network/fetch"
	"github.com/creativesoftwarefdn/weaviate/usecases/network/common/peers"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
)

func TestProxyGetInstance(t *testing.T) {
	var (
		subject *network
		remote  *httptest.Server
		err     error
	)

	arrange := func(matchers ...requestMatcher) {
		remote = fakeRemoteInstanceWithGraphQL(t, matchers...)
		subject = &network{
			peers: []peers.Peer{{
				Name: "best-instance",
				URI:  strfmt.URI(remote.URL),
				ID:   strfmt.UUID("some-id"),
			}},
		}
	}

	act := func() {
		_, err = subject.ProxyGetInstance(common.Params{
			SubQuery:       common.SubQuery(`{ Local { Get { Things { City { name } } } } }`),
			TargetInstance: "best-instance",
		})
	}

	cleanUp := func() {
		remote.Close()
	}

	t.Run("should not error", func(t *testing.T) {
		arrange()
		act()

		if err != nil {
			t.Errorf("should not error, but got %s", err)
		}

		cleanUp()
	})

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

	t.Run("should form a local query from the subquery in the request body", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			expectedBody := fmt.Sprintf("%s\n", `{"query":"{ Local { Get { Things { City { name } } } } }"}`)
			defer r.Body.Close()
			bodyBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Errorf("reading the request body returned an error: %s", err)
			}

			actualBody := string(bodyBytes)
			if actualBody != expectedBody {
				t.Errorf("expected body to be \n%#v\n, but was \n%#v\n", expectedBody, actualBody)
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})

	// re-enable when we have auth again
	// t.Run("should proxy along the key and token headers", func(t *testing.T) {
	// 	matcher := func(t *testing.T, r *http.Request) {
	// 		key := r.Header.Get("X-API-KEY")
	// 		token := r.Header.Get("X-API-TOKEN")

	// 		expectedKey := "stand-in-for-key-id-uuid"
	// 		if key != expectedKey {
	// 			t.Errorf("expected key to be \n%#v\n, but was \n%#v\n", expectedKey, key)
	// 		}

	// 		expectedToken := "stand-in-for-token-uuid"
	// 		if token != expectedToken {
	// 			t.Errorf("expected token to be \n%#v\n, but was \n%#v\n", expectedToken, token)
	// 		}
	// 	}
	// 	arrange(matcher)
	// 	act()
	// 	cleanUp()
	// })
}

func TestProxyGetMetaInstance(t *testing.T) {
	var (
		subject *network
		remote  *httptest.Server
		err     error
	)

	arrange := func(matchers ...requestMatcher) {
		remote = fakeRemoteInstanceWithGraphQL(t, matchers...)
		subject = &network{
			peers: []peers.Peer{{
				Name: "best-instance",
				URI:  strfmt.URI(remote.URL),
				ID:   strfmt.UUID("some-id"),
			}},
		}
	}

	act := func() {
		_, err = subject.ProxyGetMetaInstance(common.Params{
			SubQuery:       common.SubQuery(`{ Local { GetMeta { WeaviateB { Things { City { meta { count } } } } } }`),
			TargetInstance: "best-instance",
		})
	}

	cleanUp := func() {
		remote.Close()
	}

	t.Run("should not error", func(t *testing.T) {
		arrange()
		act()

		if err != nil {
			t.Errorf("should not error, but got %s", err)
		}

		cleanUp()
	})

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

	t.Run("should form a local query from the subquery in the request body", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			expectedBody := fmt.Sprintf("%s\n", `{"query":"{ Local { GetMeta { WeaviateB { Things { City { meta { count } } } } } }"}`)
			defer r.Body.Close()
			bodyBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Errorf("reading the request body returned an error: %s", err)
			}

			actualBody := string(bodyBytes)
			if actualBody != expectedBody {
				t.Errorf("expected body to be \n%#v\n, but was \n%#v\n", expectedBody, actualBody)
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})

	// re-enable when we have auth again
	// t.Run("should proxy along the key and token headers", func(t *testing.T) {
	// 	matcher := func(t *testing.T, r *http.Request) {
	// 		key := r.Header.Get("X-API-KEY")
	// 		token := r.Header.Get("X-API-TOKEN")

	// 		expectedKey := "stand-in-for-key-id-uuid"
	// 		if key != expectedKey {
	// 			t.Errorf("expected key to be \n%#v\n, but was \n%#v\n", expectedKey, key)
	// 		}

	// 		expectedToken := "stand-in-for-token-uuid"
	// 		if token != expectedToken {
	// 			t.Errorf("expected token to be \n%#v\n, but was \n%#v\n", expectedToken, token)
	// 		}
	// 	}
	// 	arrange(matcher)
	// 	act()
	// 	cleanUp()
	// })
}

func TestProxyAggregateInstance(t *testing.T) {
	var (
		subject *network
		remote  *httptest.Server
		err     error
	)

	arrange := func(matchers ...requestMatcher) {
		remote = fakeRemoteInstanceWithGraphQL(t, matchers...)
		subject = &network{
			peers: []peers.Peer{{
				Name: "best-instance",
				URI:  strfmt.URI(remote.URL),
				ID:   strfmt.UUID("some-id"),
			}},
		}
	}

	act := func() {
		_, err = subject.ProxyAggregateInstance(common.Params{
			SubQuery:       common.SubQuery(`{ Local { Aggregate { WeaviateB { Things { City { population { count } } } } } }`),
			TargetInstance: "best-instance",
		})
	}

	cleanUp := func() {
		remote.Close()
	}

	t.Run("should not error", func(t *testing.T) {
		arrange()
		act()

		if err != nil {
			t.Errorf("should not error, but got %s", err)
		}

		cleanUp()
	})

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

	t.Run("should form a local query from the subquery in the request body", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			expectedBody := fmt.Sprintf("%s\n", `{"query":"{ Local { Aggregate { WeaviateB { Things { City { population { count } } } } } }"}`)
			defer r.Body.Close()
			bodyBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Errorf("reading the request body returned an error: %s", err)
			}

			actualBody := string(bodyBytes)
			if actualBody != expectedBody {
				t.Errorf("expected body to be \n%#v\n, but was \n%#v\n", expectedBody, actualBody)
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})

	// re-enable when we have auth again
	// t.Run("should proxy along the key and token headers", func(t *testing.T) {
	// 	matcher := func(t *testing.T, r *http.Request) {
	// 		key := r.Header.Get("X-API-KEY")
	// 		token := r.Header.Get("X-API-TOKEN")

	// 		expectedKey := "stand-in-for-key-id-uuid"
	// 		if key != expectedKey {
	// 			t.Errorf("expected key to be \n%#v\n, but was \n%#v\n", expectedKey, key)
	// 		}

	// 		expectedToken := "stand-in-for-token-uuid"
	// 		if token != expectedToken {
	// 			t.Errorf("expected token to be \n%#v\n, but was \n%#v\n", expectedToken, token)
	// 		}
	// 	}
	// 	arrange(matcher)
	// 	act()
	// 	cleanUp()
	// })
}

func TestProxyFetch(t *testing.T) {
	var (
		subject *network
		remote1 *httptest.Server
		remote2 *httptest.Server
		err     error
		results []fetch.Response
	)

	arrange := func(matchers ...requestMatcher) {
		remote1 = fakeRemoteInstanceWithGraphQL(t, matchers...)
		remote2 = fakeRemoteInstanceWithGraphQL(t, matchers...)
		subject = &network{
			peers: []peers.Peer{{
				Name: "best-instance",
				URI:  strfmt.URI(remote1.URL),
				ID:   strfmt.UUID("some-id"),
			}, {
				Name: "worst-instance",
				URI:  strfmt.URI(remote2.URL),
				ID:   strfmt.UUID("some-other-id"),
			}},
		}
	}

	act := func() {
		results, err = subject.ProxyFetch(
			common.SubQuery(`{ Local { Aggregate { WeaviateB { Things { City { population { count } } } } } }`),
		)
	}

	cleanUp := func() {
		remote1.Close()
		remote2.Close()
	}

	t.Run("should not error", func(t *testing.T) {
		arrange()
		act()

		if err != nil {
			t.Errorf("should not error, but got %s", err)
		}

		cleanUp()
	})

	t.Run("handler should be called", func(t *testing.T) {
		called := false
		m := &sync.Mutex{}
		matcher := func(t *testing.T, r *http.Request) {
			m.Lock()
			defer m.Unlock()
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

	t.Run("should return the correct results", func(t *testing.T) {
		arrange()
		act()
		_ = err
		assert.Len(t, results, 2)
		cleanUp()
	})

	t.Run("should form a local query from the subquery in the request body", func(t *testing.T) {
		matcher := func(t *testing.T, r *http.Request) {
			expectedBody := fmt.Sprintf("%s\n", `{"query":"{ Local { Aggregate { WeaviateB { Things { City { population { count } } } } } }"}`)
			defer r.Body.Close()
			bodyBytes, err := ioutil.ReadAll(r.Body)
			if err != nil {
				t.Errorf("reading the request body returned an error: %s", err)
			}

			actualBody := string(bodyBytes)
			if actualBody != expectedBody {
				t.Errorf("expected body to be \n%#v\n, but was \n%#v\n", expectedBody, actualBody)
			}
		}
		arrange(matcher)
		act()
		cleanUp()
	})

	// re-enable when we have auth again
	// t.Run("should proxy along the key and token headers", func(t *testing.T) {
	// 	matcher := func(t *testing.T, r *http.Request) {
	// 		key := r.Header.Get("X-API-KEY")
	// 		token := r.Header.Get("X-API-TOKEN")

	// 		expectedKey := "stand-in-for-key-id-uuid"
	// 		if key != expectedKey {
	// 			t.Errorf("expected key to be \n%#v\n, but was \n%#v\n", expectedKey, key)
	// 		}

	// 		expectedToken := "stand-in-for-token-uuid"
	// 		if token != expectedToken {
	// 			t.Errorf("expected token to be \n%#v\n, but was \n%#v\n", expectedToken, token)
	// 		}
	// 	}
	// 	arrange(matcher)
	// 	act()
	// 	cleanUp()
	// })
}

type requestMatcher func(t *testing.T, r *http.Request)

func fakeRemoteInstanceWithGraphQL(t *testing.T, matchers ...requestMatcher) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, matcher := range matchers {
			matcher(t, r)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "%s", `{}`)
	}))
	return ts
}
