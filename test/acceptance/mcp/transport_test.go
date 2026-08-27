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
	"strings"
	"testing"
	"time"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

// TestMCPTransport checks what a raw HTTP client sees on /v1/mcp: GET is
// refused, no session id is issued, requests work with or without one, and
// the MCP-Protocol-Version header is checked.
func TestMCPTransport(t *testing.T) {
	// A session id as issued by older, stateful builds. Clients that connected
	// before an upgrade keep sending it.
	const staleSessionID = "mcp-session-0b1c9f5e-1111-2222-3333-444444444444"
	const toolsListBody = `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
	const getConfigBody = `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"weaviate-collections-get-config","arguments":{}}}`

	newCtx := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		t.Cleanup(cancel)
		return ctx
	}
	requireTools := func(t *testing.T, body []byte) {
		var listed struct {
			Result struct {
				Tools []struct {
					Name string `json:"name"`
				} `json:"tools"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal(body, &listed))
		require.NotEmpty(t, listed.Result.Tools)
	}

	t.Run("GET is refused with 405", func(t *testing.T) {
		status, headers, _ := helper.RawMCPRequest(newCtx(t), t, http.MethodGet, testMCPURL, testAPIKey, "", "")
		require.Equal(t, http.StatusMethodNotAllowed, status)
		allowed := strings.Split(headers.Get("Allow"), ",")
		for i := range allowed {
			allowed[i] = strings.TrimSpace(allowed[i])
		}
		require.ElementsMatch(t, []string{http.MethodPost, http.MethodDelete}, allowed)
	})

	t.Run("initialize issues no session id", func(t *testing.T) {
		status, headers, _ := helper.RawMCPRequest(newCtx(t), t, http.MethodPost, testMCPURL, testAPIKey, helper.MCPInitializeBody, "")
		require.Equal(t, http.StatusOK, status)
		require.Empty(t, headers.Get("Mcp-Session-Id"))
	})

	t.Run("tools/list works without a session id", func(t *testing.T) {
		status, _, body := helper.RawMCPRequest(newCtx(t), t, http.MethodPost, testMCPURL, testAPIKey, toolsListBody, "")
		require.Equal(t, http.StatusOK, status, string(body))
		requireTools(t, body)
	})

	t.Run("tools/list works with a session id from an older build", func(t *testing.T) {
		status, _, body := helper.RawMCPRequest(newCtx(t), t, http.MethodPost, testMCPURL, testAPIKey, toolsListBody, staleSessionID)
		require.Equal(t, http.StatusOK, status, string(body))
		requireTools(t, body)
	})

	t.Run("DELETE is accepted with and without a session id", func(t *testing.T) {
		for _, sessionID := range []string{"", staleSessionID} {
			status, _, body := helper.RawMCPRequest(newCtx(t), t, http.MethodDelete, testMCPURL, testAPIKey, "", sessionID)
			require.Equal(t, http.StatusOK, status, string(body))
		}
	})

	t.Run("a protocol version header alone is enough for tools/list and tools/call", func(t *testing.T) {
		versionHeader := http.Header{"MCP-Protocol-Version": {mcplib.LATEST_PROTOCOL_VERSION}}

		status, _, body := helper.RawMCPRequest(newCtx(t), t, http.MethodPost, testMCPURL, testAPIKey, toolsListBody, "", versionHeader)
		require.Equal(t, http.StatusOK, status, string(body))
		requireTools(t, body)

		status, _, body = helper.RawMCPRequest(newCtx(t), t, http.MethodPost, testMCPURL, testAPIKey, getConfigBody, "", versionHeader)
		require.Equal(t, http.StatusOK, status, string(body))
		var called struct {
			Result struct {
				IsError bool `json:"isError"`
			} `json:"result"`
			Error any `json:"error"`
		}
		require.NoError(t, json.Unmarshal(body, &called))
		require.Nil(t, called.Error, string(body))
		require.False(t, called.Result.IsError, string(body))
	})

	t.Run("an unsupported protocol version is refused with 400", func(t *testing.T) {
		bogus := http.Header{"MCP-Protocol-Version": {"bogus"}}
		status, _, body := helper.RawMCPRequest(newCtx(t), t, http.MethodPost, testMCPURL, testAPIKey, toolsListBody, "", bogus)
		require.Equal(t, http.StatusBadRequest, status, string(body))
		require.JSONEq(t, `{"error":"unsupported MCP protocol version: bogus"}`, string(body))
	})
}
