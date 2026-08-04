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
	"regexp"
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

// maxReindexProbes caps the probe loop. The leading probe already decides the
// verdict, so the remaining ones only widen the diagnostic on a failure.
const maxReindexProbes = 10

// slowBackupConfig widens the window an in-flight backup can be observed in:
// CPUPercentage 1 collapses the zip onto a single goroutine and
// BestCompression raises the CPU cost per byte.
func slowBackupConfig() *models.BackupConfig {
	return &models.BackupConfig{
		CompressionLevel: models.BackupConfigCompressionLevelBestCompression,
		CPUPercentage:    1,
	}
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

// reindexProbe is one reindex submission taken while the coordinator still
// reported the backup as in flight.
type reindexProbe struct {
	backupStatus string
	httpStatus   int
	body         string
}

// probeRun carries the failed status reads alongside the probes so a run that
// produced no probe at all can be told apart from a broken status endpoint.
type probeRun struct {
	probes       []reindexProbe
	statusReads  int
	statusErrors int
	lastStatus   string
}

// probeReindexDuringBackup submits the reindex repeatedly for as long as
// statusOf reports a non-terminal backup, stopping at the first 409.
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

// assertReindexBlocked judges a probe run against the guard contract and
// returns the 409 it found.
//
// Only the leading probe decides the verdict. The coordinator refreshes
// participant status every 2s to 10s, so a late 202 can mean the backup had
// already released its slot rather than that the guard has a gap. The first
// probe lands microseconds after the create call returned, at which point
// every participant provably holds a slot because canCommit runs before the
// create call answers.
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
	require.Containsf(t, message, backupID,
		"409 body must name the blocking backup id %q; got: %s", backupID, first.body)
	return first
}

// guardMessage flattens the error payload so assertions read the message text
// rather than its JSON escaping.
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

var blockingNodePattern = regexp.MustCompile(`node "([^"]+)"`)

// blockingNodeName pulls the node the guard named out of a 409 body.
func blockingNodeName(t *testing.T, body string) string {
	t.Helper()
	match := blockingNodePattern.FindStringSubmatch(guardMessage(body))
	require.Lenf(t, match, 2,
		`409 body must name the blocking node as node "<name>"; got: %s`, body)
	return match[1]
}

// localBackupStatus reads the create status through the process-global client,
// which is the single node under test.
func localBackupStatus(t *testing.T, backend, backupID string) func() (string, bool) {
	return func() (string, bool) {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		if err != nil || resp == nil || resp.Payload == nil || resp.Payload.Status == nil {
			return "", false
		}
		return *resp.Payload.Status, true
	}
}

// nodeBackupStatus reads the create status straight off one node, which is what
// a cluster test needs because the shared client targets a single global host.
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

// awaitBackupSuccess blocks until the backup reports SUCCESS, failing on any
// other terminal status.
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

// tryReindexSubmit is the tolerant sibling of
// reindexhelpers.SubmitIndexUpdateExpect4xx: a transport error is a retryable
// observation rather than a test failure, because the node may still be booting.
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

// awaitReindexAccepted polls the submission until it is accepted and returns
// the task id. Used where the block is expected to lift with no operator action.
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

// awaitNodeServing blocks until the node answers for the class again, so a
// post-restart deadline measures the block lifting rather than container boot.
func awaitNodeServing(t *testing.T, restURI, className string, deadline time.Duration) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		// consistency:false reads the node's own schema view, which is what the
		// submission handler validates against.
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

// awaitSingleShardOwner blocks until exactly one node reports a shard of the
// class and returns its name.
func awaitSingleShardOwner(t *testing.T, restURI, className string, deadline time.Duration) string {
	t.Helper()
	var owner string
	var last []string
	// assert.Eventually plus an explicit Fatalf, because require.Eventually
	// captures its message arguments before the first poll and could not
	// report the ownership it actually saw.
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

// raftLeaderName resolves the current RAFT leader, which the backup coordinator
// always enrolls as a participant regardless of shard ownership.
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

// startS3Backup fires the create call and returns as soon as the coordinator
// accepted it, leaving the transfer in flight for the probe loop.
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

// getJSON GETs url and decodes into out, reporting false on any failed step so
// it composes inside polling loops.
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

// dumpWeaviateLogs prints the tail of a container log on failure so the server
// side of the story survives the run.
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
