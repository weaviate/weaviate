package clusterapi

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_staticRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/indices", okHandler)
	mux.HandleFunc("/replicas/", okHandler)

	cases := []struct {
		name     string
		req      *http.Request
		expected string
	}{
		{
			name:     "unmatched route",
			req:      newRequest(t, "/foo"), // un-matched route
			expected: "/foo",
		},
		{
			name:     "matched route",
			req:      newRequest(t, "/indices"), // matched route
			expected: "/indices",
		},
		{
			name:     "un-matched route with dynamic path",
			req:      newRequest(t, "/indices/objects/Movies"), // un-matched route. Note original handler is `/indices` (without `/` suffix)
			expected: "/indices/objects/Movies",
		},
		{
			name:     "matched route with dynamic path",
			req:      newRequest(t, "/replicas/objects/Movies"), // matched route.
			expected: "/replicas/",                              // yay!
		},
		{
			name:     "matched route with dynamic path 2",
			req:      newRequest(t, "/replicas/objects/Movies2"), // matched route.
			expected: "/replicas/",                               // yay!
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := staticRoute(mux)(tc.req)
			fmt.Println("got", got)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func newRequest(t *testing.T, path string) *http.Request {
	t.Helper()

	r, err := http.NewRequest("GET", path, nil)
	require.NoError(t, err)
	return r
}

func okHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
	w.WriteHeader(http.StatusOK)
}
