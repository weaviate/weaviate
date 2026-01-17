//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package read

import (
	"context"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// FetchLogs retrieves recent server logs from the buffer hook with pagination support
func (r *WeaviateReader) FetchLogs(ctx context.Context, req mcp.CallToolRequest, args FetchLogsArgs) (*FetchLogsResp, error) {
	// Authorize the request
	_, err := r.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}

	// Check if log buffer hook is available
	if r.logBuffer == nil {
		return &FetchLogsResp{
			Logs: "Log buffer not available. Ensure MCP_SERVER_READ_LOGS_ENABLED is set to true when starting the server.",
		}, nil
	}

	// Set default limit if not specified
	limit := args.Limit
	if limit <= 0 {
		limit = 2000
	}

	// Enforce maximum limit
	const maxLimit = 50000
	if limit > maxLimit {
		limit = maxLimit
	}

	// Set default offset if not specified
	offset := args.Offset
	if offset < 0 {
		offset = 0
	}

	// Fetch logs from the buffer hook with pagination
	logs := r.logBuffer.GetLogs(offset, limit)

	return &FetchLogsResp{
		Logs: logs,
	}, nil
}
