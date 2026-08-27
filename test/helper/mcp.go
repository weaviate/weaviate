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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
)

// CallToolOnce calls an MCP tool once with the default admin-key authentication
func CallToolOnce[I any, O any](ctx context.Context, t *testing.T, tool string, in I, out *O, apiKey string) error {
	var opts []transport.StreamableHTTPCOption
	if apiKey != "" {
		opts = append(opts, transport.WithHTTPHeaders(map[string]string{
			"Authorization": "Bearer " + apiKey,
		}))
	}

	client, err := client.NewStreamableHttpClient(GetWeaviateURL()+"/v1/mcp", opts...)
	if err != nil {
		return err
	}

	_, err = client.Initialize(ctx, mcp.InitializeRequest{})
	if err != nil {
		return err
	}

	res, err := client.CallTool(ctx, mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      tool,
			Arguments: in,
		},
	})
	if err != nil {
		return err
	}
	require.NotNil(t, res)
	if res.IsError {
		// Extract error message from Content
		if len(res.Content) > 0 {
			if textContent, ok := mcp.AsTextContent(res.Content[0]); ok {
				return fmt.Errorf("%s", textContent.Text)
			}
		}
		return fmt.Errorf("tool returned error")
	}
	require.NotNil(t, res.StructuredContent)

	bytes, err := json.Marshal(res.StructuredContent)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, out)
	if err != nil {
		return err
	}
	return nil
}

// MCPInitializeBody is a minimal initialize request for raw calls to /v1/mcp.
const MCPInitializeBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0.0"}}}`

// RawMCPRequest sends one plain HTTP request to /v1/mcp and returns the status,
// headers and body, which the MCP client would mask. A GET asks for an event
// stream, as a real client would. A non-empty sessionID is sent as
// Mcp-Session-Id.
func RawMCPRequest(ctx context.Context, t *testing.T, method, mcpURL, apiKey, body, sessionID string) (int, http.Header, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, mcpURL, reader)
	require.NoError(t, err)
	accept := "application/json, text/event-stream"
	if method == http.MethodGet {
		accept = "text/event-stream"
	}
	req.Header.Set("Accept", accept)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header, respBody
}
