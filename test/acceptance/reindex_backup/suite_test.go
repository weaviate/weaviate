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

// Package reindex_backup_test covers the backup × runtime-reindex
// interaction: in-flight reindex rejects a backup with a structured
// error naming the blocking tracker; quiet state round-trips cleanly.
package reindex_backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clientbackups "github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

// TestBackupVsReindexSuite drives all reindex-backup interaction
// scenarios on a shared single-node testcontainer. Subtests use distinct
// class names so .migrations/ state cannot bleed between cases.
func TestBackupVsReindexSuite(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		// USE_INVERTED_SEARCHABLE=false forces new classes to start with
		// the legacy Map (WAND) strategy so the rebuild:true verb has
		// actual Map→Blockmax migration work to do.
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()
	restURI := compose.GetWeaviate().URI()

	// Dump container logs on failure.
	container := compose.GetWeaviate().Container()
	defer func() {
		if t.Failed() {
			reader, err := container.Logs(ctx)
			if err != nil {
				t.Logf("failed to read container logs: %v", err)
				return
			}
			defer reader.Close()
			logs, _ := io.ReadAll(reader)
			lines := strings.Split(string(logs), "\n")
			if len(lines) > 200 {
				lines = lines[len(lines)-200:]
			}
			t.Logf("=== Container logs (last 200 lines) ===\n%s", strings.Join(lines, "\n"))
		}
	}()

	t.Run("BaselineBackupRoundTrip", func(t *testing.T) {
		testBaselineBackupRoundTrip(t, restURI)
	})

	t.Run("BackupRefusedDuringInFlightMigration", func(t *testing.T) {
		testBackupRefusedDuringInFlightMigration(t, ctx, compose, restURI)
	})

	t.Run("BackupSucceedsAfterMigrationFinishes", func(t *testing.T) {
		testBackupSucceedsAfterMigrationFinishes(t, restURI)
	})

	t.Run("PostRestartOrphanAuditClearsTracker", func(t *testing.T) {
		testPostRestartOrphanAuditClearsTracker(t, ctx, compose, restURI)
	})

	// The subtests below re-resolve URI each call because
	// PostRestartOrphanAuditClearsTracker above does a Stop+Start that
	// rebinds the container to a new dynamic port.
	t.Run("CancelOnNoInFlightReturns202NoOp", func(t *testing.T) {
		testCancelOnNoInFlightReturns202NoOp(t, compose.GetWeaviate().URI())
	})

	t.Run("AlgorithmVerbRefusesOnAlreadyBlockmaxRejectsWAND", func(t *testing.T) {
		testAlgorithmVerb(t, compose.GetWeaviate().URI())
	})

	t.Run("MutationGuardBlocksDeleteClassDuringInFlight", func(t *testing.T) {
		testMutationGuardBlocksDeleteClassDuringInFlight(t, compose.GetWeaviate().URI())
	})

	t.Run("CancelClearsTrackerDirsViaOnTaskCompleted", func(t *testing.T) {
		testCancelClearsTrackerDirsViaOnTaskCompleted(t, ctx, compose, compose.GetWeaviate().URI())
	})
}

// testBaselineBackupRoundTrip backs up, deletes, and restores a class
// with no migration in flight, asserting the object count round-trips.
func testBaselineBackupRoundTrip(t *testing.T, restURI string) {
	className := "ReindexBackup_Baseline"
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	importBodies(t, className, 200)

	preCount := moduleshelper.GetClassCount(t, className, "")
	require.EqualValues(t, 200, preCount)

	backupAndRestoreRoundTrip(t, className, "reindex-backup-baseline", preCount,
		"restored count must equal pre-backup count")
}

// testBackupRefusedDuringInFlightMigration asserts that a backup fired
// during an in-flight change-tokenization reindex is rejected
// synchronously with a structured 422 naming the active tracker, and
// that the same backup id succeeds once the migration drains.
func testBackupRefusedDuringInFlightMigration(t *testing.T, ctx context.Context, compose *docker.DockerCompose, restURI string) {
	className := "ReindexBackup_RefusedDuringInFlight"
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	// 50k objects sized so the iteration runs ~5-15s on CI, giving the
	// indexes status poll time to observe the indexing state and the
	// backup HTTP call time to land mid-iteration.
	importBodies(t, className, 50_000)

	taskID := submitChangeTokenization(t, restURI, className, "body", "lowercase")
	t.Logf("change-tokenization task submitted: %s", taskID)

	// Without this wait, a small corpus can finish the migration before
	// the backup HTTP call lands, producing a spurious "backup succeeded".
	awaitIndexingState(t, restURI, className, "body")

	backupID := "reindex-backup-refuse"

	// The Backupable precheck refuses during canCommit before the
	// coordinator writes backup_config.json, so the HTTP call returns
	// a synchronous 422 — no async status poll required.
	_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, "filesystem", backupID)
	require.Error(t, err, "create-backup must be refused synchronously while reindex is in flight")
	var refusal *clientbackups.BackupsCreateUnprocessableEntity
	require.ErrorAs(t, err, &refusal, "expected 422 BackupsCreateUnprocessableEntity, got %T: %v", err, err)
	require.NotNil(t, refusal.Payload, "422 payload must not be nil")
	require.NotEmpty(t, refusal.Payload.Error, "422 ErrorResponse must surface the refusal reason")
	errMsg := errorResponseMessage(refusal.Payload)

	require.Contains(t, errMsg, "backup blocked: runtime-reindex in flight on this shard",
		"error body must name the blocking condition; got: %s", errMsg)
	require.Contains(t, errMsg, className,
		"error body must name the affected collection; got: %s", errMsg)
	require.Contains(t, errMsg, "active runtime-reindex task in DTM",
		"error body must explain the gate consulted DTM; got: %s", errMsg)
	require.Contains(t, errMsg, "retry after the migration finishes",
		"error body must include an actionable next step")

	// The refusal must not leave a staging dir behind; otherwise a
	// same-id retry hits checkIfBackupExists's "Status != Cancelled"
	// rejection.
	container := compose.GetWeaviate().Container()
	stagingPath := "/tmp/backups/" + backupID
	code, _, _ := container.Exec(ctx, []string{"test", "-d", stagingPath})
	assert.NotEqual(t, 0, code,
		"refused backup must not leave a staging dir at %s", stagingPath)

	// Wait via the index-status surface (status:"ready") rather than DTM
	// task status. With the DTM-backed gate, FINISHED already releases
	// the gate, but status:"ready" additionally confirms the underlying
	// index has flipped to the new tokenization, which is closer to the
	// queryable end-state operators care about.
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, "body", "searchable",
		reindexhelpers.WithTimeout(120*time.Second))

	// Same-id retry must now succeed cleanly.
	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), className, "filesystem", backupID)
	require.NoError(t, err, "same-id retry after migration drains must succeed")
	helper.ExpectBackupEventuallyCreated(t, backupID, "filesystem", nil,
		helper.WithDeadline(2*time.Minute))
}

// errorResponseMessage flattens an ErrorResponse into a single string
// for substring assertions.
func errorResponseMessage(er *models.ErrorResponse) string {
	if er == nil {
		return ""
	}
	parts := make([]string, 0, len(er.Error))
	for _, e := range er.Error {
		if e == nil {
			continue
		}
		parts = append(parts, e.Message)
	}
	return strings.Join(parts, "; ")
}

// testBackupSucceedsAfterMigrationFinishes asserts that once the
// migration completes, backup-create + restore work cleanly — i.e. the
// rejection path didn't leave the per-shard halt counter incremented.
func testBackupSucceedsAfterMigrationFinishes(t *testing.T, restURI string) {
	className := "ReindexBackup_SucceedsAfterFinish"
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	importBodies(t, className, 500)
	preCount := moduleshelper.GetClassCount(t, className, "")
	require.EqualValues(t, 500, preCount)

	taskID := submitChangeTokenization(t, restURI, className, "body", "lowercase")
	t.Logf("change-tokenization task submitted: %s", taskID)
	// Use the index-status surface (status:"ready") rather than DTM
	// FINISHED: both signal the gate is open, but status:"ready" is
	// closer to operator expectations and survives future DTM-status
	// reshuffles.
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, className, "body", "searchable",
		reindexhelpers.WithTimeout(60*time.Second))

	backupAndRestoreRoundTrip(t, className, "reindex-backup-after-finish", preCount,
		"post-migration backup must round-trip object count cleanly")
}

// importBodies inserts `count` objects with a `body` text property long
// enough that the change-tokenization iteration has meaningful work per
// object.
func importBodies(t *testing.T, className string, count int) {
	t.Helper()
	const batchSize = 500
	body := strings.Repeat("Alpha Bravo Charlie Delta Echo Foxtrot Golf Hotel ", 4)
	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}
		objects := make([]*models.Object, end-start)
		for i := range objects {
			objects[i] = &models.Object{
				Class:      className,
				ID:         strfmt.UUID(uuid.New().String()),
				Properties: map[string]interface{}{"body": body},
			}
		}
		params := batch.NewBatchObjectsCreateParams().
			WithBody(batch.BatchObjectsCreateBody{Objects: objects})
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		require.NoError(t, err)
		require.NotNil(t, resp)
	}
}

// submitChangeTokenization issues PUT /v1/schema/<class>/indexes/<prop>
// with {"searchable":{"tokenization":<target>}}, asserts 202, and
// returns the task id.
func submitChangeTokenization(t *testing.T, restURI, collection, property, target string) string {
	t.Helper()
	body := fmt.Sprintf(`{"searchable":{"tokenization":%q}}`, target)
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"index update returned non-202: %s", string(respBody))

	type body202 struct {
		TaskID string `json:"taskId"`
		Status string `json:"status"`
	}
	var parsed body202
	require.NoError(t, json.Unmarshal(respBody, &parsed))
	require.NotEmpty(t, parsed.TaskID, "submit response missing taskId: %s", string(respBody))
	return parsed.TaskID
}

// testPostRestartOrphanAuditClearsTracker injects an orphan tracker
// dir + sidecar bucket on disk (the shape a pre-fix backup-restore
// would leave), restarts the container, and asserts the post-bootstrap
// audit removes both while leaving the canonical bucket and data intact.
func testPostRestartOrphanAuditClearsTracker(t *testing.T, ctx context.Context, compose *docker.DockerCompose, restURI string) {
	const (
		className   = "ReindexBackup_OrphanAudit"
		shardLookup = "ReindexBackup_OrphanAudit"
	)

	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	importBodies(t, className, 500)
	preCount := moduleshelper.GetClassCount(t, className, "")
	require.EqualValues(t, 500, preCount)

	shardName := reindexhelpers.GetFirstShardName(t, restURI, className)
	require.NotEmpty(t, shardName, "could not resolve shard name for %s", className)

	// Stop, stage orphan state, restart so the audit fires on it.
	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())
	container := compose.GetWeaviate().Container()

	require.EqualValues(t, preCount, moduleshelper.GetClassCount(t, className, ""),
		"baseline restart must not lose data")

	lsmPath := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), shardName)
	orphanDir := "searchable_retokenize_body_999" // gen 999 is far outside any runtime-picked value
	sidecarBucket := "property_body_searchable__retokenize_reindex_999"
	injectOrphanTrackerOnDisk(t, ctx, container, lsmPath, orphanDir, sidecarBucket,
		`{"taskID":"orphan-from-prefix-backup","taskVersion":1,"unitID":"u0","payload":{"collection":"`+className+`","migrationType":"change-tokenization","properties":["body"],"targetTokenization":"lowercase","bucketStrategy":"map_collection"}}`)

	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())
	container = compose.GetWeaviate().Container()

	// The audit runs async after meta store ready + DTM bootstrap.
	require.Eventually(t, func() bool {
		code, _, _ := container.Exec(ctx, []string{
			"test", "-d",
			filepath.Join(lsmPath, ".migrations", orphanDir),
		})
		return code != 0
	}, 60*time.Second, 500*time.Millisecond,
		"orphan tracker dir was not cleaned up by the post-bootstrap audit")

	code, _, _ := container.Exec(ctx, []string{"test", "-d", filepath.Join(lsmPath, sidecarBucket)})
	assert.NotEqual(t, 0, code,
		"orphan sidecar bucket dir was not cleaned up; got test -d exit %d", code)

	assert.EqualValues(t, preCount, moduleshelper.GetClassCount(t, className, ""),
		"canonical data must survive the audit")
}

// testCancelOnNoInFlightReturns202NoOp asserts the M6 contract:
// PUT {"searchable":{"cancel":true}} with no task targeting the tuple
// returns 202 Accepted with Status: NO_OP and no TaskID — cancel is
// idempotent on the "nothing to cancel" path. Matches the singlenode
// copy of the test in test/acceptance/reindex_singlenode/cancel_test.go.
func testCancelOnNoInFlightReturns202NoOp(t *testing.T, restURI string) {
	const (
		className = "ReindexBackup_CancelNoTask"
		propName  = "body"
	)
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	// Defensive: some handlers short-circuit on empty classes.
	importBodies(t, className, 5)

	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, propName)
	req, err := http.NewRequest(http.MethodPut, url,
		bytes.NewReader([]byte(`{"searchable":{"cancel":true}}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equalf(t, http.StatusAccepted, resp.StatusCode,
		"cancel-with-no-task should 202 NO_OP; got %d: %s", resp.StatusCode, string(respBody))

	var result models.IndexUpdateResponse
	require.NoError(t, json.Unmarshal(respBody, &result),
		"cancel-no-task response body should decode as IndexUpdateResponse: %s", string(respBody))
	assert.Equal(t, "NO_OP", result.Status,
		"cancel-no-task should report Status: NO_OP, got body: %s", string(respBody))
	assert.Empty(t, result.TaskID,
		"cancel-no-task should not name a TaskID, got body: %s", string(respBody))
}

// testAlgorithmVerb asserts that on an already-blockmax class:
//   - searchable.algorithm:"blockmax" → 400 (already on blockmax)
//   - searchable.algorithm:"WAND"     → 422 (swagger enum validator)
//
// and that neither refusal schedules a DTM task.
func testAlgorithmVerb(t *testing.T, restURI string) {
	const (
		className = "ReindexBackup_AlgorithmVerb"
		propName  = "body"
	)
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, className)

	importBodies(t, className, 10)

	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, propName)

	// USE_INVERTED_SEARCHABLE=false starts the class on Map (WAND); migrate
	// it to blockmax via the algorithm verb so the refusal cases below
	// have an already-blockmax target.
	preTaskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, propName,
		`{"searchable":{"algorithm":"blockmax"}}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, preTaskID,
		reindexhelpers.WithTimeout(60*time.Second))

	preTasksResp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	require.NoError(t, err)
	preTasksBytes, _ := io.ReadAll(preTasksResp.Body)
	_ = preTasksResp.Body.Close()

	// algorithm:"blockmax" on already-blockmax → 400.
	req, err := http.NewRequest(http.MethodPut, url,
		bytes.NewReader([]byte(`{"searchable":{"algorithm":"blockmax"}}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	bodyBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equalf(t, http.StatusBadRequest, resp.StatusCode,
		"algorithm:blockmax on an already-blockmax class must be refused with 400; got %d: %s", resp.StatusCode, string(bodyBytes))
	assert.Contains(t, string(bodyBytes), "already on blockmax",
		"400 body must explain the refusal reason")

	// algorithm:"WAND" → 422 (rejected at the swagger enum-validator layer,
	// which fires before the handler — the schema is enum:["blockmax"]).
	req, err = http.NewRequest(http.MethodPut, url,
		bytes.NewReader([]byte(`{"searchable":{"algorithm":"WAND"}}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	bodyBytes, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equalf(t, http.StatusUnprocessableEntity, resp.StatusCode,
		"WAND algorithm must be rejected with 422 at the swagger validator; got %d: %s", resp.StatusCode, string(bodyBytes))
	assert.Contains(t, string(bodyBytes), "blockmax",
		"422 body must name the only accepted enum value")

	postTasksResp, err := http.Get(fmt.Sprintf("http://%s/v1/tasks", restURI))
	require.NoError(t, err)
	defer postTasksResp.Body.Close()
	postTasksBytes, err := io.ReadAll(postTasksResp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, string(preTasksBytes), string(postTasksBytes),
		"refused submits must not schedule any new DTM task. preTaskID=%s. pre=%s post=%s",
		preTaskID, string(preTasksBytes), string(postTasksBytes))
}

// testMutationGuardBlocksDeleteClassDuringInFlight asserts that
// DELETE /v1/schema/<class> while a reindex on `body` is STARTED is
// rejected with a structured 400 naming the in-flight task, and that
// DELETE succeeds once the task finishes.
func testMutationGuardBlocksDeleteClassDuringInFlight(t *testing.T, restURI string) {
	const (
		className = "ReindexBackup_MutationGuard"
		propName  = "body"
	)
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	// The test drives the final DELETE itself; defer is just a bail-out
	// cleanup for aborted runs.
	deletedByTest := false
	defer func() {
		if !deletedByTest {
			helper.DeleteClass(t, className)
		}
	}()

	importBodies(t, className, 50_000)

	taskID := submitChangeTokenization(t, restURI, className, propName, "lowercase")
	t.Logf("change-tokenization task submitted for mutation-guard probe: %s", taskID)

	awaitIndexingState(t, restURI, className, propName)

	deleteURL := fmt.Sprintf("http://%s/v1/schema/%s", restURI, className)
	req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	bodyBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	require.Equalf(t, http.StatusBadRequest, resp.StatusCode,
		"DELETE during in-flight reindex must be refused with 400; got %d: %s",
		resp.StatusCode, string(bodyBytes))

	bodyStr := string(bodyBytes)
	assert.Contains(t, bodyStr, "in flight",
		"400 body must explain that a task is in flight; got: %s", bodyStr)
	assert.Contains(t, bodyStr, className,
		"400 body must name the class being deleted; got: %s", bodyStr)
	assert.Contains(t, bodyStr, "cancel",
		"400 body must point operators at the cancel remedy; got: %s", bodyStr)

	getURL := fmt.Sprintf("http://%s/v1/schema/%s", restURI, className)
	resp, err = http.Get(getURL)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"class must survive a guard-rejected DELETE; got %d on GET", resp.StatusCode)

	reindexhelpers.AwaitReindexFinished(t, restURI, taskID, reindexhelpers.WithTimeout(120*time.Second))

	req, err = http.NewRequest(http.MethodDelete, deleteURL, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"post-tidied DELETE must succeed; got %d", resp.StatusCode)
	deletedByTest = true
}

// testCancelClearsTrackerDirsViaOnTaskCompleted asserts two contracts:
//
//  1. Cancel triggers auto-cleanup so `.migrations/<prefix>_body_N/`
//     drains from disk within a few scheduler ticks.
//  2. DELETE class after the task reaches CANCELLED succeeds and
//     leaves no on-disk class dir behind.
func testCancelClearsTrackerDirsViaOnTaskCompleted(t *testing.T, ctx context.Context, compose *docker.DockerCompose, restURI string) {
	const (
		className = "ReindexBackup_CancelCleanup"
		propName  = "body"
	)
	helper.CreateClass(t, &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	})
	// The test drives the final DELETE itself; defer is just a bail-out
	// cleanup for aborted runs.
	deletedByTest := false
	defer func() {
		if !deletedByTest {
			helper.DeleteClass(t, className)
		}
	}()

	importBodies(t, className, 50_000)

	taskID := reindexhelpers.SubmitIndexUpdate(t, restURI, className, propName,
		`{"searchable":{"tokenization":"lowercase"}}`)
	t.Logf("cancel-cleanup probe task submitted: %s", taskID)

	awaitIndexingState(t, restURI, className, propName)

	cancelURL := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, className, propName)
	req, err := http.NewRequest(http.MethodPut, cancelURL,
		bytes.NewReader([]byte(`{"searchable":{"cancel":true}}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	cancelBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equalf(t, http.StatusAccepted, resp.StatusCode,
		"cancel must return 202; got %d: %s", resp.StatusCode, string(cancelBody))

	shardName := reindexhelpers.GetFirstShardName(t, restURI, className)
	lsmPath := fmt.Sprintf("/data/%s/%s/lsm", strings.ToLower(className), shardName)
	migsPath := lsmPath + "/.migrations"
	container := compose.GetWeaviate().Container()
	classPath := fmt.Sprintf("/data/%s", strings.ToLower(className))

	// Poll .migrations/ until every body-related dir is gone (cleanup
	// runs async on the scheduler tick).
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, reader, execErr := container.Exec(ctx, []string{
			"sh", "-c",
			fmt.Sprintf(`ls -1 %s 2>/dev/null | grep -E '_%s($|_)' | head -10`, migsPath, propName),
		})
		require.NoError(t, execErr)
		out := new(strings.Builder)
		if reader != nil {
			_, _ = io.Copy(out, reader)
		}
		matches := strings.TrimSpace(out.String())
		// grep exit 1 (no match) or empty stdout means cleanup is done.
		if code != 0 || matches == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cancel-cleanup did not remove %s/.migrations/*_%s_* within 30s; survivors:\n%s",
				lsmPath, propName, matches)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// MutationGuard's IsActive() gate (STARTED/PREPARING/SWAPPING only)
	// means CANCELLED does not block DELETE.
	deleteURL := fmt.Sprintf("http://%s/v1/schema/%s", restURI, className)
	delReq, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	require.NoError(t, err)
	delResp, err := http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	delBody, _ := io.ReadAll(delResp.Body)
	_ = delResp.Body.Close()
	require.Equalf(t, http.StatusOK, delResp.StatusCode,
		"DELETE class after CANCELLED task must succeed; got %d: %s",
		delResp.StatusCode, string(delBody))
	deletedByTest = true

	code, _, execErr := container.Exec(ctx, []string{"test", "-d", classPath})
	require.NoError(t, execErr)
	require.Equalf(t, 1, code,
		"class dir %s must be removed by DELETE; got test -d exit %d",
		classPath, code)

	_ = taskID
}

// backupAndRestoreRoundTrip creates a filesystem backup, deletes the
// class, restores it, and asserts the post-restore count equals
// preCount.
func backupAndRestoreRoundTrip(t *testing.T, className, backupID string, preCount int64, msg string) {
	t.Helper()
	_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, "filesystem", backupID)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, backupID, "filesystem", nil,
		helper.WithDeadline(2*time.Minute))

	helper.DeleteClass(t, className)
	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, "filesystem", backupID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, backupID, "filesystem", nil,
		helper.WithDeadline(2*time.Minute))

	postCount := moduleshelper.GetClassCount(t, className, "")
	assert.Equal(t, preCount, postCount, msg)
}

// injectOrphanTrackerOnDisk crafts the on-disk shape that a pre-fix
// backup-restore would leave on a restored shard:
//
//   - .migrations/<orphanDir>/started.mig
//   - .migrations/<orphanDir>/reindexed.mig
//   - .migrations/<orphanDir>/payload.mig (with the supplied JSON body)
//   - .migrations/<orphanDir>/audit_quarantined.mig (mtime pre-aged
//     well past `reindexAuditQuarantineWindow` so the audit's S2
//     two-pass safeguard collapses to a single destructive sweep —
//     otherwise the test would need to wait the full quarantine
//     window for a second audit pass that doesn't fire post-bootstrap)
//   - <sidecarBucket>/marker.flag
func injectOrphanTrackerOnDisk(t *testing.T, ctx context.Context, container testcontainers.Container,
	lsmPath, orphanDir, sidecarBucket, payloadJSON string,
) {
	t.Helper()
	trackerDir := filepath.Join(lsmPath, ".migrations", orphanDir)
	// Compute the pre-aged timestamp host-side in POSIX touch -t form
	// (YYYYMMDDhhmm.ss) so the inject works on the alpine/busybox base
	// of the testcontainer (busybox touch lacks GNU `-d` relative dates).
	agedTs := time.Now().Add(-time.Hour).UTC().Format("200601021504.05")
	for _, cmd := range [][]string{
		{"mkdir", "-p", trackerDir},
		{"touch", filepath.Join(trackerDir, "started.mig")},
		{"touch", filepath.Join(trackerDir, "reindexed.mig")},
		{"sh", "-c", fmt.Sprintf("cat > %s <<'EOF'\n%s\nEOF", filepath.Join(trackerDir, "payload.mig"), payloadJSON)},
		// Pre-aged quarantine sentinel: mirror the unit-test
		// `writePreAgedQuarantineSentinel` helper. Without this,
		// PostRestartOrphanAuditClearsTracker would race the 5-minute
		// quarantine window and time out (the test only waits 60s).
		{"touch", "-t", agedTs, filepath.Join(trackerDir, "audit_quarantined.mig")},
		{"mkdir", "-p", filepath.Join(lsmPath, sidecarBucket)},
		{"touch", filepath.Join(lsmPath, sidecarBucket, "marker.flag")},
	} {
		execInContainer(t, ctx, container, cmd...)
	}
}

// execInContainer runs a command inside the testcontainer and fails
// the test on non-zero exit.
func execInContainer(t *testing.T, ctx context.Context, c testcontainers.Container, cmd ...string) {
	t.Helper()
	code, reader, err := c.Exec(ctx, cmd)
	require.NoError(t, err, "exec %v", cmd)
	output := ""
	if reader != nil {
		buf := new(strings.Builder)
		_, _ = io.Copy(buf, reader)
		output = buf.String()
	}
	require.Equal(t, 0, code, "exec %v exited %d; output: %s", cmd, code, output)
}

// awaitIndexingState polls GET /v1/schema/<class>/indexes until at
// least one index for the named property reports status "indexing".
// Returns without failing if the migration completes before the state
// is observed (callers see this as a too-small fixture, not a gate
// regression).
func awaitIndexingState(t *testing.T, restURI, collection, property string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/v1/schema/%s/indexes", restURI, collection))
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var parsed struct {
			Properties []struct {
				Name    string `json:"name"`
				Indexes []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"indexes"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(body, &parsed); err == nil {
			for _, p := range parsed.Properties {
				if p.Name != property {
					continue
				}
				for _, idx := range p.Indexes {
					if idx.Status == "indexing" {
						t.Logf("observed indexing state for %s/%s (type=%s)", collection, property, idx.Type)
						return
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("warning: did not observe indexing state for %s/%s within deadline; migration may have completed too fast for the test fixture", collection, property)
}
