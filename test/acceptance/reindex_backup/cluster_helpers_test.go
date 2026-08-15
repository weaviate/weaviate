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

package reindex_backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	guardClusterSize = 3
	// The reindex-target class only needs to exist.
	probeDataset = 500
	// Bounds the search for a placement the scheduler will not let us request.
	maxPlacementAttempts = 16
)

// startGuardCluster brings up three nodes behind MinIO. A cluster of two or
// more refuses the filesystem backend, so every multi-node capture here is S3.
func startGuardCluster(ctx context.Context, t *testing.T, bucket string) *docker.DockerCompose {
	t.Helper()
	compose, err := reindexhelpers.WithReindexEnv(
		docker.New().
			With3NodeCluster().
			WithBackendS3(bucket, "us-east-1"),
	).
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("MEMBERLIST_FAST_FAILURE_DETECTION", "false").
		Start(ctx)
	require.NoError(t, err)
	return compose
}

// clusterNode pairs a Weaviate node name with the URI the test reaches it on.
type clusterNode struct {
	name string
	uri  string
}

// clusterNodes lists the cluster 1-indexed, matching docker.GetWeaviateNode.
func clusterNodes(compose *docker.DockerCompose, size int) []clusterNode {
	nodes := make([]clusterNode, 0, size)
	for i := 1; i <= size; i++ {
		container := compose.GetWeaviateNode(i)
		nodes = append(nodes, clusterNode{name: container.Name(), uri: container.URI()})
	}
	return nodes
}

func nodeNames(nodes []clusterNode) []string {
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		names = append(names, n.name)
	}
	return names
}

func nodeByName(t *testing.T, nodes []clusterNode, name string) clusterNode {
	t.Helper()
	for _, n := range nodes {
		if n.name == name {
			return n
		}
	}
	t.Fatalf("node %q is not part of the cluster %v", name, nodeNames(nodes))
	return clusterNode{}
}

func dumpClusterLogs(ctx context.Context, t *testing.T, compose *docker.DockerCompose, nodes []clusterNode) {
	for i, node := range nodes {
		dumpWeaviateLogs(ctx, t, compose.GetWeaviateNode(i+1).Container(), node.name)
	}
}

// shardOwners returns the node names holding at least one shard of the class.
func shardOwners(restURI, className string) ([]string, bool) {
	var parsed struct {
		Nodes []struct {
			Name   string `json:"name"`
			Shards []struct {
				Class string `json:"class"`
			} `json:"shards"`
		} `json:"nodes"`
	}
	if !getJSON(fmt.Sprintf("http://%s/v1/nodes?output=verbose", restURI), &parsed) {
		return nil, false
	}

	var owners []string
	for _, node := range parsed.Nodes {
		for _, shard := range node.Shards {
			if shard.Class == className {
				owners = append(owners, node.Name)
				break
			}
		}
	}
	return owners, true
}

// awaitSingleShardOwner blocks until exactly one node reports a shard of the class.
func awaitSingleShardOwner(t *testing.T, restURI, className string, deadline time.Duration) string {
	t.Helper()
	var owner string
	var last []string
	// assert.Eventually plus an explicit Fatalf: require.Eventually renders its
	// message before the first poll, so it cannot report what was seen.
	resolved := assert.Eventually(t, func() bool {
		owners, ok := shardOwners(restURI, className)
		if !ok {
			return false
		}
		last = owners
		if len(owners) != 1 {
			return false
		}
		owner = owners[0]
		return true
	}, deadline, 250*time.Millisecond)
	if !resolved {
		t.Fatalf("class %s must be owned by exactly one node for a probe node to exist; "+
			"last ownership seen: %v", className, last)
	}
	return owner
}

// awaitClusterMembers blocks until every node appears in the cluster's own
// view. A single-shard class created before that lands on whichever node is
// already there, so the placement search would draw the same owner every time.
func awaitClusterMembers(t *testing.T, restURI string, want []string, deadline time.Duration) {
	t.Helper()
	var last []string
	resolved := assert.Eventually(t, func() bool {
		var parsed struct {
			Nodes []struct {
				Name string `json:"name"`
			} `json:"nodes"`
		}
		if !getJSON(fmt.Sprintf("http://%s/v1/nodes", restURI), &parsed) {
			return false
		}
		last = last[:0]
		for _, node := range parsed.Nodes {
			last = append(last, node.Name)
		}
		for _, name := range want {
			if !slices.Contains(last, name) {
				return false
			}
		}
		return true
	}, deadline, 250*time.Millisecond)
	if !resolved {
		t.Fatalf("cluster did not report all of %v within %s; last seen: %v", want, deadline, last)
	}
}

// raftLeaderName resolves the current RAFT leader, which is enrolled as a
// backup participant whatever it owns.
func raftLeaderName(t *testing.T, restURI string, deadline time.Duration) string {
	t.Helper()
	var leader string
	resolved := assert.Eventually(t, func() bool {
		var stats models.ClusterStatisticsResponse
		if !getJSON(fmt.Sprintf("http://%s/v1/cluster/statistics", restURI), &stats) {
			return false
		}
		for _, s := range stats.Statistics {
			if name, ok := s.LeaderID.(string); ok && name != "" {
				leader = name
				return true
			}
		}
		return false
	}, deadline, 250*time.Millisecond)
	if !resolved {
		t.Fatalf("/v1/cluster/statistics on %s did not report a leader within %s", restURI, deadline)
	}
	return leader
}

// awaitClassVisible blocks until the node's own schema view serves the class,
// which is what the handler under test reads.
func awaitClassVisible(t *testing.T, restURI, className, nodeName string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		_, ok := reindexhelpers.FetchClass(restURI, className, true)
		return ok
	}, 60*time.Second, 100*time.Millisecond,
		"class %s must be locally visible on %s before the test drives it", className, nodeName)
}

// createSingleShardClass puts a whole class on one node, so a test can say
// which node holds a stake in an operation and which does not.
func createSingleShardClass(t *testing.T, className, propName string) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer:        "none",
		ShardingConfig:    map[string]interface{}{"desiredCount": 1},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	})
}

// startS3Backup fires the create call and returns once accepted, leaving the
// transfer in flight.
func startS3Backup(restURI, className, backupID, bucket string) error {
	payload := map[string]interface{}{
		"id":      backupID,
		"include": []string{className},
		"config": map[string]interface{}{
			"Bucket":           bucket,
			"CPUPercentage":    1,
			"CompressionLevel": models.BackupConfigCompressionLevelBestCompression,
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/v1/backups/s3", restURI),
		"application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backup create returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// tryS3Backup returns the status and body of a create call, so a caller can
// assert on a refusal instead of only on success.
func tryS3Backup(restURI, className, backupID, bucket string) (int, string, bool) {
	payload := map[string]interface{}{
		"id":      backupID,
		"include": []string{className},
		"config":  map[string]interface{}{"Bucket": bucket},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, "", false
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/v1/backups/s3", restURI),
		"application/json", bytes.NewReader(body))
	if err != nil {
		return 0, "", false
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", false
	}
	return resp.StatusCode, string(respBody), true
}

// nodeBackupStatus reads status straight off one node, since the shared client
// only ever targets one host.
func nodeBackupStatus(restURI, backend, backupID string) func() (string, bool) {
	url := fmt.Sprintf("http://%s/v1/backups/%s/%s", restURI, backend, backupID)
	return func() (string, bool) {
		// A pointer, like the generated payload: status is omitempty, so a body
		// without it decodes cleanly into the zero value. Reporting that as a
		// good read makes the backup look live forever and makes every failed
		// read count as a successful one in the vacuity check.
		var parsed struct {
			Status *string `json:"status"`
		}
		if !getJSON(url, &parsed) || parsed.Status == nil || *parsed.Status == "" {
			return "", false
		}
		return *parsed.Status, true
	}
}
