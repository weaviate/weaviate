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

package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

// TestMCPTransport checks what a raw HTTP client sees on /v1/mcp: GET is
// refused, initialize issues no session id, and POST works without one.
func TestMCPTransport(t *testing.T) {
	// Each subtest gets its own deadline so a hanging GET cannot starve the rest.
	newCtx := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	t.Run("GET is refused with 405", func(t *testing.T) {
		ctx := newCtx(t)
		status, headers, body := helper.RawMCPRequest(ctx, t, http.MethodGet, testMCPURL, testAPIKey, "")
		require.Equal(t, http.StatusMethodNotAllowed, status)
		require.Equal(t, "POST, DELETE", headers.Get("Allow"))
		require.Equal(t, "application/json", headers.Get("Content-Type"))
		require.Contains(t, string(body), "GET is not supported")
	})

	t.Run("initialize issues no session id", func(t *testing.T) {
		ctx := newCtx(t)
		status, headers, _ := helper.RawMCPRequest(ctx, t, http.MethodPost, testMCPURL, testAPIKey, helper.MCPInitializeBody)
		require.Equal(t, http.StatusOK, status)
		require.Empty(t, headers.Get("Mcp-Session-Id"))
	})

	t.Run("tools/list succeeds without a session id", func(t *testing.T) {
		ctx := newCtx(t)
		status, _, body := helper.RawMCPRequest(ctx, t, http.MethodPost, testMCPURL, testAPIKey,
			`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
		require.Equal(t, http.StatusOK, status, string(body))

		var listed struct {
			Result struct {
				Tools []struct {
					Name string `json:"name"`
				} `json:"tools"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(body, &listed))
		require.NotEmpty(t, listed.Result.Tools)
	})
}
