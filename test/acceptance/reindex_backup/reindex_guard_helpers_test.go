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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// maxReindexProbes caps the loop; only the leading probe decides the verdict,
// the rest just add diagnostic detail.
const maxReindexProbes = 10

// slowBackupConfig widens the observable in-flight window: CPUPercentage 1
// serializes the zip and BestCompression raises the per-byte CPU cost.
func slowBackupConfig() *models.BackupConfig {
	return &models.BackupConfig{
		CompressionLevel: models.BackupConfigCompressionLevelBestCompression,
		CPUPercentage:    1,
	}
}

// createBodyClass creates an unvectorized class with a single word-tokenized
// text property, the shape every single-node guard test migrates.
func createBodyClass(t *testing.T, className, propName string) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
}

// backupTerminal reports whether a backup status can no longer change.
func backupTerminal(status string) bool {
	switch backup.Status(status) {
	case backup.Success, backup.Failed, backup.Cancelled:
		return true
	default:
		return false
	}
}

// reindexProbe is one reindex submission taken while the backup was still in flight.
type reindexProbe struct {
	backupStatus string
	httpStatus   int
	body         string
}

// probeRun tracks failed status reads too, so a zero-probe run can be told
// apart from a broken status endpoint.
type probeRun struct {
	probes       []reindexProbe
	statusReads  int
	statusErrors int
	lastStatus   string
}

// probeReindexDuringBackup submits reindex until the first 409 or the backup goes terminal.
func probeReindexDuringBackup(
	t *testing.T,
	probeURI, collection, property, targetTokenization string,
	statusOf func() (string, bool),
	deadline time.Duration,
) probeRun {
	t.Helper()

	requestBody := fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, targetTokenization)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	run := probeRun{lastStatus: "(no successful status read)"}
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		status, ok := statusOf()
		run.statusReads++
		if !ok {
			run.statusErrors++
			<-ticker.C
			continue
		}
		run.lastStatus = status
		if backupTerminal(status) {
			break
		}

		resp := reindexhelpers.SubmitIndexUpdateExpect4xx(t, probeURI, collection, property, requestBody)
		run.probes = append(run.probes, reindexProbe{
			backupStatus: status,
			httpStatus:   resp.StatusCode,
			body:         resp.Body,
		})
		if resp.StatusCode == http.StatusConflict || len(run.probes) >= maxReindexProbes {
			break
		}
		<-ticker.C
	}
	return run
}

// assertReindexBlocked judges a probe run against the guard contract: only
// the first probe must be 409. It lands microseconds after the create call
// returns, when every participant already holds a slot; later probes can 202
// legitimately once a participant's slot expires.
func assertReindexBlocked(t *testing.T, run probeRun, backupID string) reindexProbe {
	t.Helper()

	if len(run.probes) == 0 {
		t.Fatalf("vacuous run: backup %q read as %q before a single reindex submission could be "+
			"probed against it (%d status reads, %d of them failed) — grow the imported dataset "+
			"until the backup stays in flight for several seconds",
			backupID, run.lastStatus, run.statusReads, run.statusErrors)
	}

	first := run.probes[0]
	if first.httpStatus != http.StatusConflict {
		t.Fatalf("reindex submission returned %d while backup %q was %s; the guard must answer 409:\n%s",
			first.httpStatus, backupID, first.backupStatus, formatProbes(run.probes))
	}

	message := guardMessage(first.body)
	require.Containsf(t, message, "reindex blocked",
		"409 body must name the blocking condition; got: %s", first.body)
	require.Containsf(t, message, "is running in the cluster",
		"409 body must say what is blocking; got: %s", first.body)
	require.NotContainsf(t, message, backupID,
		"409 body leaked backup id %q; got: %s", backupID, first.body)
	return first
}

// guardMessage flattens the error payload to plain message text, dropping JSON escaping.
func guardMessage(body string) string {
	var payload models.ErrorResponse
	if json.Unmarshal([]byte(body), &payload) == nil {
		if message := errorResponseMessage(&payload); message != "" {
			return message
		}
	}
	return strings.ReplaceAll(body, `\"`, `"`)
}

func formatProbes(probes []reindexProbe) string {
	lines := make([]string, 0, len(probes))
	for i, p := range probes {
		lines = append(lines, fmt.Sprintf("  probe %d: backup=%s http=%d body=%s",
			i, p.backupStatus, p.httpStatus, strings.TrimSpace(p.body)))
	}
	return strings.Join(lines, "\n")
}

// localBackupStatus reads status via the process-global client (the single node under test).
func localBackupStatus(t *testing.T, backend, backupID string) func() (string, bool) {
	return func() (string, bool) {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		if err != nil || resp == nil || resp.Payload == nil || resp.Payload.Status == nil {
			return "", false
		}
		return *resp.Payload.Status, true
	}
}

// nodeBackupStatus reads status straight off one node, since the shared client only targets one host.
func nodeBackupStatus(restURI, backend, backupID string) func() (string, bool) {
	url := fmt.Sprintf("http://%s/v1/backups/%s/%s", restURI, backend, backupID)
	return func() (string, bool) {
		var parsed struct {
			Status string `json:"status"`
		}
		if !getJSON(url, &parsed) {
			return "", false
		}
		return parsed.Status, true
	}
}

// awaitBackupSuccess blocks until the backup reports SUCCESS, failing on any other terminal status.
func awaitBackupSuccess(t *testing.T, statusOf func() (string, bool), backupID string, deadline time.Duration) {
	t.Helper()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	last := "(no successful status read)"
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if status, ok := statusOf(); ok {
			last = status
			if status == string(backup.Success) {
				return
			}
			if backupTerminal(status) {
				t.Fatalf("backup %q reached terminal status %q instead of SUCCESS", backupID, status)
			}
		}
		<-ticker.C
	}
	t.Fatalf("backup %q did not reach SUCCESS within %s; last status: %q", backupID, deadline, last)
}

// tryReindexSubmit tolerates transport errors (e.g. node still booting) as a
// retryable observation, unlike reindexhelpers.SubmitIndexUpdateExpect4xx.
func tryReindexSubmit(restURI, collection, property, requestBody string) (int, string, bool) {
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(requestBody))
	if err != nil {
		return 0, "", false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", false
	}
	return resp.StatusCode, string(body), true
}

// awaitReindexAccepted polls until the submission is accepted and returns the task id.
func awaitReindexAccepted(
	t *testing.T, restURI, collection, property, targetTokenization string, deadline time.Duration,
) string {
	t.Helper()
	requestBody := fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, targetTokenization)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	last := "(no response yet)"
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		status, body, ok := tryReindexSubmit(restURI, collection, property, requestBody)
		switch {
		case !ok:
			last = "transport error"
		case status == http.StatusAccepted:
			var parsed struct {
				TaskID string `json:"taskId"`
			}
			require.NoErrorf(t, json.Unmarshal([]byte(body), &parsed),
				"202 response is not an IndexUpdateResponse: %s", body)
			require.NotEmptyf(t, parsed.TaskID, "202 response is missing taskId: %s", body)
			return parsed.TaskID
		default:
			last = fmt.Sprintf("http=%d body=%s", status, strings.TrimSpace(body))
		}
		<-ticker.C
	}
	t.Fatalf("reindex submission on %s was never accepted within %s; last response: %s",
		restURI, deadline, last)
	return ""
}

// awaitNodeServing blocks until the node serves the class again, so a
// post-restart deadline measures the block lifting, not boot time.
func awaitNodeServing(t *testing.T, restURI, className string, deadline time.Duration) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		// consistency:false reads the node's own schema view, matching what the
		// submission handler checks.
		if _, ok := reindexhelpers.FetchClass(restURI, className, true); ok {
			return
		}
		<-ticker.C
	}
	t.Fatalf("node %s did not serve class %s again within %s", restURI, className, deadline)
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
	// Uses assert.Eventually + explicit Fatalf: require.Eventually captures its
	// message before the first poll, so it can't report what ownership was seen.
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
		t.Fatalf("class %s must be owned by exactly one node for the probe node to exist; "+
			"last ownership seen: %v", className, last)
	}
	return owner
}

// awaitClusterMembers blocks until every node appears in the cluster's own
// view. A single-shard class created before that lands on whichever node is
// already there, so resolveGuardTopology draws the same owner every time and
// exhausts its attempts.
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

// raftLeaderName resolves the current RAFT leader, always enrolled as a
// backup participant regardless of shard ownership.
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

// startS3Backup fires the create call and returns once accepted, leaving the transfer in flight.
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

// getJSON GETs url into out, returning false on any failed step (composes in polling loops).
func getJSON(url string, out any) bool {
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	return json.Unmarshal(body, out) == nil
}

// dumpWeaviateLogs prints the container's last 200 log lines on test failure.
func dumpWeaviateLogs(ctx context.Context, t *testing.T, container testcontainers.Container, label string) {
	if !t.Failed() {
		return
	}
	reader, err := container.Logs(ctx)
	if err != nil {
		t.Logf("failed to read %s logs: %v", label, err)
		return
	}
	defer reader.Close()
	logs, _ := io.ReadAll(reader)
	lines := strings.Split(string(logs), "\n")
	if len(lines) > 200 {
		lines = lines[len(lines)-200:]
	}
	t.Logf("=== %s logs (last 200 lines) ===\n%s", label, strings.Join(lines, "\n"))
}
