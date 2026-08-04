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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/weaviate/weaviate/usecases/backup"
)

const pathBackupNodeActivity = "/backups/node-activity"

// ErrNodeActivityUnsupported marks a node that doesn't serve the node-activity
// route (e.g. mid rolling-upgrade); callers treat it as not busy.
var ErrNodeActivityUnsupported = errors.New("node does not serve the backup node-activity route")

// ClusterBackupActivity probes whether a node is currently part of a backup or restore.
type ClusterBackupActivity struct {
	client   *http.Client
	resolver nodeResolver
}

func NewClusterBackupActivity(client *http.Client, resolver nodeResolver) *ClusterBackupActivity {
	return &ClusterBackupActivity{client: client, resolver: resolver}
}

func (c *ClusterBackupActivity) NodeActivity(ctx context.Context, nodeName string) (backup.NodeActivity, error) {
	host, found := c.resolver.NodeHostname(nodeName)
	if !found {
		return backup.NodeActivity{}, fmt.Errorf("unable to resolve hostname for %q", nodeName)
	}

	u := url.URL{Scheme: "http", Host: host, Path: pathBackupNodeActivity}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return backup.NodeActivity{}, fmt.Errorf("new node activity request: %w", err)
	}

	res, err := c.client.Do(req)
	if err != nil {
		return backup.NodeActivity{}, fmt.Errorf("node activity request: %w", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return backup.NodeActivity{}, fmt.Errorf("read node activity response: %w", err)
	}

	if res.StatusCode == http.StatusNotFound {
		return backup.NodeActivity{}, ErrNodeActivityUnsupported
	}
	if res.StatusCode != http.StatusOK {
		return backup.NodeActivity{}, fmt.Errorf("node activity: unexpected status code %d (%s)", res.StatusCode, body)
	}

	var activity backup.NodeActivity
	if err := json.Unmarshal(body, &activity); err != nil {
		return backup.NodeActivity{}, fmt.Errorf("unmarshal node activity response: %w", err)
	}
	return activity, nil
}
