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

func (c *ClusterBackupActivity) NodeActivity(ctx context.Context, nodeName string) (backup.NodeActivity, error) {
	var res backup.NodeActivityResponse
	if err := c.getJSON(ctx, nodeName, clusterprobe.BackupNodeActivityPath,
		ErrNodeActivityUnsupported, "node activity", &res); err != nil {
		return backup.NodeActivity{}, err
	}
	activity, err := res.Activity()
	if err != nil {
		return backup.NodeActivity{}, fmt.Errorf("node activity: %w", err)
	}
	return activity, nil
}
