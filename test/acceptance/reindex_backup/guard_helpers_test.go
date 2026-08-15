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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clientbackups "github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/nodes"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// guardDataset is sized so a change-tokenization migration over it stays
// live on CI long enough to land a backup or a restore inside.
const guardDataset = 50_000

// Only the leading probe decides a verdict; the rest are diagnostic detail.
const maxReindexProbes = 10

func startGuardNode(ctx context.Context, t *testing.T) *docker.DockerCompose {
	t.Helper()
	compose, err := reindexhelpers.SingleNodeCompose().
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	return compose
}

// createBodyClass creates a single word-tokenized text property, which is
// what a change-tokenization migration needs to have work to do.
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

// createBackupOf backs up several collections in one operation, which the
// single-class helpers in test/helper cannot express.
func createBackupOf(t *testing.T, backend, backupID string, classes ...string) {
	t.Helper()
	params := clientbackups.NewBackupsCreateParams().
		WithBackend(backend).
		WithBody(&models.BackupCreateRequest{
			ID:      backupID,
			Include: classes,
			Config:  helper.DefaultBackupConfig(),
		})
	_, err := helper.Client(t).Backups.BackupsCreate(params, nil)
	require.NoError(t, err, "backup create must be accepted with no migration in flight")
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(2*time.Minute))
}

// restoreClasses restores a named subset of a backup and returns the error
// unjudged: both admitted and refused are legitimate answers to ask for.
func restoreClasses(t *testing.T, backend, backupID string, classes ...string) error {
	t.Helper()
	params := clientbackups.NewBackupsRestoreParams().
		WithBackend(backend).
		WithID(backupID).
		WithBody(&models.BackupRestoreRequest{
			Include: classes,
			Config:  helper.DefaultRestoreConfig(),
		})
	_, err := helper.Client(t).Backups.BackupsRestore(params, nil)
	return err
}

// requireNoPlacement asserts a published refusal names neither the shard nor
// the node the work is on. The resolved shard name is matched too: a leak
// worded without the quote passes the format assertion above it. An empty
// shardName is for a refusal that is collection-wide and never resolved one.
func requireNoPlacement(t *testing.T, msg, shardName string) {
	t.Helper()
	require.NotContainsf(t, msg, `shard "`, "a refusal names no shard; got: %s", msg)
	if shardName != "" {
		require.NotContainsf(t, msg, shardName, "a refusal names no shard; got: %s", msg)
	}
	clusterNodes, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
	require.NoError(t, err)
	for _, node := range clusterNodes.Payload.Nodes {
		require.NotContainsf(t, msg, node.Name, "a refusal names no node; got: %s", msg)
	}
}

// reindexTaskStatus reads a task's DTM status, so a test can prove the
// migration was still live on both sides of the window it tested.
func reindexTaskStatus(t *testing.T, restURI, taskID string) string {
	t.Helper()
	tasks, ok := reindexhelpers.TryFetchTasks(restURI)
	require.True(t, ok, "tasks endpoint must answer")
	for _, namespaced := range tasks {
		for _, task := range namespaced {
			if task.ID == taskID {
				return task.Status
			}
		}
	}
	t.Fatalf("task %s not found in /v1/tasks", taskID)
	return ""
}

// liveReindexStatus mirrors the server's own liveness predicate: a status
// this build does not recognize counts as live, because the alternative
// reads a newer node's migration as finished.
func liveReindexStatus(status string) bool {
	switch status {
	case "FINISHED", "CANCELLED", "FAILED":
		return false
	default:
		return true
	}
}

// slowBackupConfig throttles the upload so a test has time to submit a
// migration before the backup commits: CPUPercentage 1 serializes the zip and
// BestCompression raises the per-byte CPU cost. It is a widened window, not a
// barrier: descriptor generation is unthrottled hardlinking and finishes long
// before the throttled upload does, so throttling only the upload is exactly
// what makes the per-shard gate lose the race to the commit-time check.
func slowBackupConfig() *models.BackupConfig {
	return &models.BackupConfig{
		CompressionLevel: models.BackupConfigCompressionLevelBestCompression,
		CPUPercentage:    1,
	}
}

func backupTerminal(status string) bool {
	switch entitiesbackup.Status(status) {
	case entitiesbackup.Success, entitiesbackup.Failed, entitiesbackup.Cancelled:
		return true
	default:
		return false
	}
}

// backupSnapshot is the whole status payload, not just the status string:
// judging whether a capture and a migration overlapped needs the
// server-stamped window and the failure reason as well.
type backupSnapshot struct {
	status      string
	startedAt   time.Time
	completedAt time.Time
	errMessage  string
}

func (s backupSnapshot) terminal() bool { return backupTerminal(s.status) }

// localBackupSnapshot reads the whole status payload off the node under test.
func localBackupSnapshot(t *testing.T, backend, backupID string) func() (backupSnapshot, bool) {
	return func() (backupSnapshot, bool) {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		if err != nil || resp == nil || resp.Payload == nil || resp.Payload.Status == nil {
			return backupSnapshot{}, false
		}
		payload := resp.Payload
		return backupSnapshot{
			status:      *payload.Status,
			startedAt:   time.Time(payload.StartedAt),
			completedAt: time.Time(payload.CompletedAt),
			errMessage:  payload.Error,
		}, true
	}
}

func localBackupStatus(t *testing.T, backend, backupID string) func() (string, bool) {
	snapshotOf := localBackupSnapshot(t, backend, backupID)
	return func() (string, bool) {
		snap, ok := snapshotOf()
		return snap.status, ok
	}
}

// localRestoreStatus is the restore-side twin of localBackupStatus.
func localRestoreStatus(t *testing.T, backend, backupID string) func() (string, bool) {
	return func() (string, bool) {
		resp, err := helper.RestoreBackupStatus(t, backend, backupID, "", "")
		if err != nil || resp == nil || resp.Payload == nil || resp.Payload.Status == nil {
			return "", false
		}
		return *resp.Payload.Status, true
	}
}

// awaitBackupTerminal polls until the backup can no longer change AND the node
// has released its slot. Both are needed: while the slot is held, status is
// served from memory with no completion time and no failure reason.
func awaitBackupTerminal(t *testing.T, snapshotOf func() (backupSnapshot, bool), deadline time.Duration) backupSnapshot {
	t.Helper()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var last backupSnapshot
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if snap, ok := snapshotOf(); ok {
			last = snap
			if snap.terminal() && !snap.completedAt.IsZero() {
				return snap
			}
		}
		<-ticker.C
	}
	t.Fatalf("backup did not reach a terminal status with a completion time within %s "+
		"(last status %q, reason %q)", deadline, last.status, last.errMessage)
	return last
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
			if status == string(entitiesbackup.Success) {
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

// reindexProbe is one submission taken while a backup or restore was in flight.
type reindexProbe struct {
	backupStatus string
	httpStatus   int
	body         string
}

// probeRun keeps failed status reads too, so a zero-probe run can be told
// apart from a broken status endpoint.
type probeRun struct {
	probes       []reindexProbe
	statusReads  int
	statusErrors int
	lastStatus   string
}

// probeReindexDuringBackup submits until the first 409 or the operation goes
// terminal.
func probeReindexDuringBackup(
	t *testing.T,
	probeURI, collection, property, targetTokenization string,
	statusOf func() (string, bool),
	deadline time.Duration,
) probeRun {
	t.Helper()

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

		httpStatus, body, ok := tryReindexSubmit(probeURI, collection, property, targetTokenization)
		if !ok {
			<-ticker.C
			continue
		}
		run.probes = append(run.probes, reindexProbe{
			backupStatus: status, httpStatus: httpStatus, body: body,
		})
		if httpStatus == http.StatusConflict || len(run.probes) >= maxReindexProbes {
			break
		}
		<-ticker.C
	}
	return run
}

// assertReindexBlocked judges a probe run against the gate's contract: only the
// first probe must be 409. It lands microseconds after the create call returns,
// when every participant still holds a slot; a later probe can legitimately be
// admitted once a participant's slot expires.
func assertReindexBlocked(t *testing.T, run probeRun, operationID string) reindexProbe {
	t.Helper()

	if len(run.probes) == 0 {
		t.Fatalf("vacuous run: operation %q read as %q before a single reindex submission could be "+
			"probed against it (%d status reads, %d of them failed) — grow the imported dataset "+
			"until it stays in flight for several seconds",
			operationID, run.lastStatus, run.statusReads, run.statusErrors)
	}

	first := run.probes[0]
	if first.httpStatus != http.StatusConflict {
		t.Fatalf("reindex submission returned %d while operation %q was %s; the gate must answer 409:\n%s",
			first.httpStatus, operationID, first.backupStatus, formatProbes(run.probes))
	}

	message := guardMessage(first.body)
	require.Containsf(t, message, "reindex blocked",
		"409 body must name the blocking condition; got: %s", first.body)
	require.Containsf(t, message, "is running in the cluster",
		"409 body must say what is blocking; got: %s", first.body)
	require.NotContainsf(t, message, operationID,
		"409 body leaked the operation id %q; got: %s", operationID, first.body)
	return first
}

// guardMessage flattens an error payload to plain message text.
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
		lines = append(lines, fmt.Sprintf("  probe %d: operation=%s http=%d body=%s",
			i, p.backupStatus, p.httpStatus, strings.TrimSpace(p.body)))
	}
	return strings.Join(lines, "\n")
}

// heldBackupSlotRefusal reports whether a non-202 is the submit gate's own,
// transient refusal. A node releases its backup slot just after it publishes
// SUCCESS, so a submission following a backup can legitimately be refused for a
// moment. Any other refusal is the answer, not a step towards one.
func heldBackupSlotRefusal(httpStatus int, body string) bool {
	return httpStatus == http.StatusConflict &&
		strings.Contains(guardMessage(body), "is running in the cluster")
}

// tryReindexSubmit treats a transport error as a retryable observation rather
// than a failure, which a node still booting produces.
func tryReindexSubmit(restURI, collection, property, targetTokenization string) (int, string, bool) {
	url := fmt.Sprintf("http://%s/v1/schema/%s/properties/%s/index/searchable",
		restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url,
		strings.NewReader(fmt.Sprintf(`{"tokenization":%q}`, targetTokenization)))
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

// awaitReindexAccepted submits until the request is admitted, riding out the
// submit gate's own refusal and nothing else, and returns the task id.
func awaitReindexAccepted(
	t *testing.T, restURI, collection, property, targetTokenization string, deadline time.Duration,
) string {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	last := "(no response yet)"
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		httpStatus, body, ok := tryReindexSubmit(restURI, collection, property, targetTokenization)
		switch {
		case !ok:
			last = "transport error"
		case httpStatus == http.StatusAccepted:
			return taskIDOf(t, body)
		default:
			require.Truef(t, heldBackupSlotRefusal(httpStatus, body),
				"submission on %s.%s was refused with %d for a reason that will not clear on its own: %s",
				collection, property, httpStatus, strings.TrimSpace(body))
			last = fmt.Sprintf("http=%d body=%s", httpStatus, strings.TrimSpace(body))
		}
		<-ticker.C
	}
	t.Fatalf("reindex submission on %s was never accepted within %s; last response: %s",
		restURI, deadline, last)
	return ""
}

func taskIDOf(t *testing.T, body string) string {
	t.Helper()
	var parsed struct {
		TaskID string `json:"taskId"`
	}
	require.NoErrorf(t, json.Unmarshal([]byte(body), &parsed),
		"202 response is not an IndexUpdateResponse: %s", body)
	require.NotEmptyf(t, parsed.TaskID, "202 response is missing taskId: %s", body)
	return parsed.TaskID
}

// reindexTaskStartedAt reads a task's DTM start time. It is stamped by the same
// node that stamps the capture window, so the two order against each other with
// no clock skew to reason about.
func reindexTaskStartedAt(t *testing.T, restURI, taskID string) time.Time {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	end := time.Now().Add(30 * time.Second)
	for time.Now().Before(end) {
		if tasks, ok := reindexhelpers.TryFetchTasks(restURI); ok {
			for _, task := range tasks["reindex"] {
				if task.ID == taskID && !time.Time(task.StartedAt).IsZero() {
					return time.Time(task.StartedAt)
				}
			}
		}
		<-ticker.C
	}
	t.Fatalf("the admitted task %q never appeared in GET /v1/tasks with a start time, so it cannot "+
		"be ordered against the capture window", taskID)
	return time.Time{}
}

// awaitNodeServing blocks until the node serves the class again, so a
// post-restart deadline measures the block lifting, not boot time.
func awaitNodeServing(t *testing.T, restURI, className string, deadline time.Duration) {
	t.Helper()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		// The node's own schema view, matching what the submit handler reads.
		if _, ok := reindexhelpers.FetchClass(restURI, className, true); ok {
			return
		}
		<-ticker.C
	}
	t.Fatalf("node %s did not serve class %s again within %s", restURI, className, deadline)
}

// getJSON composes into polling loops: any failed step is just "not yet".
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

// dumpWeaviateLogs prints a container's last 200 log lines on test failure.
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
