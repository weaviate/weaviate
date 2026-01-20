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

	client, err := client.NewStreamableHttpClient("http://localhost:9000/mcp", opts...)
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
