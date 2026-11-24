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

package helper

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRuntimeIdleConnTimeoutBelowServer pins that the helper clients drop idle
// connections before the server does. With http.DefaultTransport (90s) they
// could reuse a connection the server had already closed after 10s, and a POST
// on it failed with a bare EOF.
func TestRuntimeIdleConnTimeoutBelowServer(t *testing.T) {
	// Default of the server's CleanupTimeout, which it uses as its idle
	// timeout; see adapters/handlers/rest/server.go.
	const serverIdleTimeout = 10 * time.Second

	rt := newRuntime(nil, "localhost:8080", "http")
	transport, ok := rt.Transport.(*http.Transport)
	require.True(t, ok, "unexpected transport type %T", rt.Transport)
	assert.Positive(t, transport.IdleConnTimeout)
	assert.Less(t, transport.IdleConnTimeout, serverIdleTimeout)

	other := newRuntime(nil, "localhost:8081", "http")
	assert.Same(t, transport, other.Transport, "helper clients share one connection pool")
}
