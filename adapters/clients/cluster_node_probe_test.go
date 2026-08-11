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
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

// Both probe clients share one skeleton, so the answers that decide whether a
// caller keeps asking a node are exercised once, here.
//
// Two answers are terminal: the node itself said it cannot serve the route.
// Everything else has to stay a plain error, because reading it as terminal
// lets a caller proceed as if the node had answered "nothing running".
func TestProbeAnswersThatEndTheConversation(t *testing.T) {
	tests := []struct {
		name    string
		respond func(w http.ResponseWriter)
		// terminal means the client returns the "stop asking" sentinel rather
		// than a retryable error.
		terminal   bool
		wantErrMsg string
	}{
		{
			name: "node's own catch-all 404",
			respond: func(w http.ResponseWriter) {
				http.Error(w, "404 page not found", http.StatusNotFound)
			},
			terminal: true,
		},
		{
			name: "node serves the route with nothing behind it",
			respond: func(w http.ResponseWriter) {
				http.Error(w, clusterprobe.ProbeNotWiredMarker, http.StatusServiceUnavailable)
			},
			terminal: true,
		},
		{
			name: "proxy-shaped 404 with an HTML body",
			respond: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><head><title>404 Not Found</title></head></html>"))
			},
			wantErrMsg: "404 did not come from the node itself",
		},
		{
			name: "404 with the node's body but no nosniff header",
			respond: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("404 page not found\n"))
			},
			wantErrMsg: "404 did not come from the node itself",
		},
		{
			name: "503 without the sentinel stays retryable",
			respond: func(w http.ResponseWriter) {
				http.Error(w, "upstream connect error", http.StatusServiceUnavailable)
			},
			wantErrMsg: "unexpected status code 503",
		},
		{
			name: "503 carrying the sentinel but no nosniff header",
			respond: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(clusterprobe.ProbeNotWiredMarker))
			},
			wantErrMsg: "unexpected status code 503",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tt.respond(w)
			}))
			defer server.Close()

			client := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))
			got, err := client.CleanupInProgress(context.Background(), "node1", "Movies")

			require.Error(t, err)
			assert.False(t, got, "no rejected answer may read as free")
			if tt.terminal {
				require.ErrorIs(t, err, ErrReindexCleanupUnsupported)
				return
			}
			require.NotErrorIs(t, err, ErrReindexCleanupUnsupported,
				"only the node itself may end the conversation")
			assert.Contains(t, err.Error(), tt.wantErrMsg)
		})
	}
}

// A peer can only make this node buffer a bounded amount, whatever it claims to
// be answering with.
func TestProbeRejectsOversizedResponse(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "at the cap", size: maxProbeResponseBytes},
		{name: "one byte over the cap", size: maxProbeResponseBytes + 1, wantErr: true},
		{name: "far over the cap", size: 4 * maxProbeResponseBytes, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Padding inside the JSON, so an accepted body still decodes and the
			// test cannot pass for the wrong reason.
			const envelope = `{"probe":"weaviate/reindex-cleanup-activity","cleaningUp":true,"pad":""}`
			require.Greater(t, tt.size, len(envelope))
			body := strings.Replace(envelope, `"pad":""`,
				`"pad":"`+strings.Repeat("a", tt.size-len(envelope))+`"`, 1)
			require.Len(t, body, tt.size)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(body))
			}))
			defer server.Close()

			client := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))
			got, err := client.CleanupInProgress(context.Background(), "node1", "Movies")

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "response exceeds")
				assert.False(t, got)
				return
			}
			require.NoError(t, err)
			assert.True(t, got)
		})
	}
}

// snippet() puts an untrusted body into an error message, so it must not cut a
// multi-byte character in half and leave invalid UTF-8 in a log line.
//
// The ascii prefixes shift where the cap lands inside the following character,
// so between them every byte of a two- and a three-byte rune is the one the cut
// would fall on.
func TestSnippetCutsOnRuneBoundaries(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "short body is kept whole", body: "not found"},
		{name: "ascii is cut at the cap", body: strings.Repeat("a", 500)},
		{name: "two-byte runes, cap on a boundary", body: strings.Repeat("é", 200)},
		{name: "two-byte runes, cap one byte in", body: "a" + strings.Repeat("é", 200)},
		{name: "three-byte runes, cap on a boundary", body: strings.Repeat("日", 200)},
		{name: "three-byte runes, cap one byte in", body: "a" + strings.Repeat("日", 200)},
		{name: "three-byte runes, cap two bytes in", body: "aa" + strings.Repeat("日", 200)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := snippet([]byte(tt.body))

			kept := strings.TrimSuffix(got, "...")
			assert.True(t, strings.HasPrefix(tt.body, kept), "the snippet has to be a prefix of the body")
			assert.True(t, utf8.ValidString(kept), "a cut must not split a rune")
		})
	}
}
