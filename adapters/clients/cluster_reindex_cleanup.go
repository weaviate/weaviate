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
	"net/http"
	"net/url"
)

const pathReindexCleanupActivity = "/reindex/cleanup-activity"

// ErrReindexCleanupUnsupported means the node runs a build without the route.
// Waiting on it would never succeed, so callers stop asking rather than burn
// their whole budget on a node that can never answer.
var ErrReindexCleanupUnsupported = errors.New("node does not serve the reindex cleanup-activity route")

type ClusterReindexCleanup struct {
	nodeProbe
}

func NewClusterReindexCleanup(client *http.Client, resolver nodeResolver) *ClusterReindexCleanup {
	return &ClusterReindexCleanup{nodeProbe{client: client, resolver: resolver}}
}

// CleanupInProgress asks one node whether it has seen a cancel for the
// collection or is still tearing down its reindex sidecars.
func (c *ClusterReindexCleanup) CleanupInProgress(ctx context.Context, nodeName, collection string) (bool, error) {
	var activity struct {
		CleaningUp bool `json:"cleaningUp"`
	}
	query := url.Values{"collection": []string{collection}}
	if err := c.getJSON(ctx, nodeName, pathReindexCleanupActivity, query,
		ErrReindexCleanupUnsupported, "reindex cleanup", &activity); err != nil {
		return false, err
	}
	return activity.CleaningUp, nil
}
