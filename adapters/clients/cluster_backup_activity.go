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

	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/backup"
)

const pathBackupNodeActivity = clusterprobe.BackupNodeActivityPath

// ErrNodeActivityUnsupported means the node runs a build without the
// node-activity route, e.g. mid rolling-upgrade. Waiting on it would never
// succeed, so callers stop asking rather than burn their whole budget on a node
// that can never answer. A node that serves the route but has no probe behind
// it answers a plain 503 instead: unlike the reindex probe, nothing here has
// established what a caller may safely assume about such a node.
var ErrNodeActivityUnsupported = errors.New("node does not serve the backup node-activity route")

// ClusterBackupActivity probes whether a node is currently part of a backup or restore.
type ClusterBackupActivity struct {
	nodeProbe
}

func NewClusterBackupActivity(client *http.Client, resolver nodeResolver) *ClusterBackupActivity {
	return &ClusterBackupActivity{nodeProbe{client: client, resolver: resolver}}
}

func (c *ClusterBackupActivity) NodeActivity(ctx context.Context, nodeName string) (backup.NodeActivity, error) {
	var res backup.NodeActivityResponse
	if err := c.getJSON(ctx, nodeName, pathBackupNodeActivity, nil,
		ErrNodeActivityUnsupported, "node activity", &res); err != nil {
		return backup.NodeActivity{}, err
	}
	activity, err := res.Activity()
	if err != nil {
		return backup.NodeActivity{}, fmt.Errorf("node activity: %w", err)
	}
	return activity, nil
}
