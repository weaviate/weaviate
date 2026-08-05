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

	"github.com/weaviate/weaviate/usecases/backup"
)

const pathBackupNodeActivity = "/backups/node-activity"

// ErrNodeActivityUnsupported marks a node that doesn't serve the node-activity
// route (e.g. mid rolling-upgrade); callers treat it as not busy.
var ErrNodeActivityUnsupported = errors.New("node does not serve the backup node-activity route")

// ClusterBackupActivity probes whether a node is currently part of a backup or restore.
type ClusterBackupActivity struct {
	nodeProbe
}

func NewClusterBackupActivity(client *http.Client, resolver nodeResolver) *ClusterBackupActivity {
	return &ClusterBackupActivity{nodeProbe{client: client, resolver: resolver}}
}

func (c *ClusterBackupActivity) NodeActivity(ctx context.Context, nodeName string) (backup.NodeActivity, error) {
	var activity backup.NodeActivity
	if err := c.getJSON(ctx, nodeName, pathBackupNodeActivity, nil,
		ErrNodeActivityUnsupported, "node activity", &activity); err != nil {
		return backup.NodeActivity{}, err
	}
	return activity, nil
}
