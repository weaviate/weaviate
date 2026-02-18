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

package export

import (
	"context"
)

// ExportClient is the HTTP client interface for inter-node export communication.
// It sends fire-and-forget requests to participant nodes.
type ExportClient interface {
	Execute(ctx context.Context, host string, req *ExportRequest) error
	IsRunning(ctx context.Context, host, exportID string) (bool, error)
}

// NodeResolver resolves node names to hostnames.
type NodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
}

// ExportStatusResponse is the JSON payload for GET /exports/status.
type ExportStatusResponse struct {
	Running bool `json:"running"`
}
