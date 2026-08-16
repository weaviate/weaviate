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
	"time"

	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// ErrNodeActivityUnsupported is the one error a caller may read as a pass, and
// only as a rolling-upgrade allowance: gating on it would refuse every submission
// until the last node is upgraded. Only a 404 written by net/http itself at the
// node's own address returns it; every other error means "cannot tell".
var ErrNodeActivityUnsupported = errors.New("node runs a build from before the backup " +
	"node-activity route, so it cannot say whether a backup is running")

// ClusterBackupActivity asks a node whether it is part of a backup or restore.
type ClusterBackupActivity struct {
	nodeProbe
}

// NewClusterBackupActivity owns a connection pool, so build it once per process.
func NewClusterBackupActivity(authConfig cluster.AuthConfig, probeTimeout time.Duration,
	resolver nodeResolver,
) *ClusterBackupActivity {
	return &ClusterBackupActivity{nodeProbe{
		client:   probeHTTPClient(authConfig, probeTimeout),
		resolver: resolver,
	}}
}

// NodeActivity asks one node whether it holds a backup or restore slot. Gate on
// [backup.NodeActivity.Free], never on Busy: every error returns the zero
// NodeActivity, so a caller that drops the error still cannot clear a node.
func (c *ClusterBackupActivity) NodeActivity(ctx context.Context, nodeName string) (backup.NodeActivity, error) {
	var res backup.NodeActivityResponse
	if err := c.getJSON(ctx, nodeName, clusterprobe.BackupNodeActivityPath,
		ErrNodeActivityUnsupported, "node activity", &res); err != nil {
		return backup.NodeActivity{}, err
	}
	activity, err := res.Activity(nodeName)
	if err != nil {
		return backup.NodeActivity{}, fmt.Errorf("node activity: %w", err)
	}
	return activity, nil
}
