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
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/weaviate/weaviate/entities/clusterprobe"
)

const pathReindexCleanupActivity = clusterprobe.ReindexCleanupActivityPath

// ErrReindexCleanupUnsupported means the node can never answer: either it runs
// a build without the route, or it serves the route with no cleanup side wired
// up yet. Callers stop asking rather than retry forever.
var ErrReindexCleanupUnsupported = errors.New("node does not serve the reindex cleanup-activity route")

type ClusterReindexCleanup struct {
	nodeProbe
}

// NewClusterReindexCleanup builds the probe client. Pass
// appState.ClusterHttpClient; see nodeProbe for why any other client fails the
// caller's gate closed.
func NewClusterReindexCleanup(client *http.Client, resolver nodeResolver) *ClusterReindexCleanup {
	return &ClusterReindexCleanup{nodeProbe{client: client, resolver: resolver}}
}

// CleanupInProgress asks one node whether it has seen a cancel for the
// collection or is still tearing down its reindex sidecars.
func (c *ClusterReindexCleanup) CleanupInProgress(ctx context.Context, nodeName, collection string) (bool, error) {
	// The same type the handler marshals, so a tag change cannot land on one
	// side only; see [clusterprobe.ReindexCleanupActivity].
	var activity clusterprobe.ReindexCleanupActivity
	query := url.Values{"collection": []string{collection}}
	if err := c.getJSON(ctx, nodeName, pathReindexCleanupActivity, query,
		ErrReindexCleanupUnsupported, "reindex cleanup", &activity); err != nil {
		return false, err
	}
	cleaningUp, err := activity.InProgress()
	if err != nil {
		return false, fmt.Errorf("reindex cleanup: %w", err)
	}
	return cleaningUp, nil
}
