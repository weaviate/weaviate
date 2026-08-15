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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clientbackups "github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	// holdWindowTenants is how many shards the cleanup has to walk: the sweep
	// runs once per (property, index type) and each run walks every local shard.
	holdWindowTenants = 150
	// holdWindowObjectsPerTenant only has to keep the migration alive long
	// enough to reach the tracker count below, which is a cheaper job than
	// widening the teardown and a different one.
	holdWindowObjectsPerTenant = 40
	// holdWindowTrackerShards is how many shards must already carry a tracker
	// dir when the cancel lands. See awaitTrackerDirs.
	holdWindowTrackerShards = holdWindowTenants * 2 / 3
)

// TestRestoreRefusedByCleanupHold pins the node-local arm of the restore
// gate, which nothing else in CI reaches: deleting the SetReindexHoldLookup
// wiring leaves every other test green.
//
// A cancelled migration goes terminal in DTM while its temporary index
// files are still on disk. The cluster-wide arm has stopped answering by
// then, so a refusal in this window can only have come from the hold.
//
// The hold is only observable if the cleanup has something to clean up;
// see awaitTrackerDirs.
func TestRestoreRefusedByCleanupHold(t *testing.T) {
	// TODO(weaviate/0-weaviate-issues#590): delete once this has been seen green.
	t.Skip("never observed passing; see weaviate/0-weaviate-issues#590")
	ctx := context.Background()
	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		className = "RestoreHold_Migrating"
		propName  = "body"
		backend   = "filesystem"
		// Never created. The unknown-id path is side-effect free, so it can
		// be retried at speed; a real restore would create the class on its
		// first admitted attempt and stop being a probe.
		unknownBackupID = "restore-hold-probe-no-such-backup"
	)

	createHoldWindowClass(t, className, propName)
	t.Cleanup(func() { helper.DeleteClass(t, className) })

	taskID := submitChangeTokenization(t, restURI, className, propName, "lowercase")
	t.Logf("hold-window task submitted: %s", taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(120*time.Second))
	awaitTrackerDirs(t, ctx, compose.GetWeaviate().Container(), className, propName,
		holdWindowTrackerShards, 120*time.Second)
	reindexhelpers.CancelIndexRaw(t, restURI, className, propName, "searchable")

	awaitReindexTaskTerminal(t, restURI, taskID, 120*time.Second)

	refusal := pollForHoldRefusal(t, backend, unknownBackupID, className, 120*time.Second)
	require.NotEmptyf(t, refusal,
		"the teardown window closed before any probe landed; raise holdWindowTrackerShards "+
			"and holdWindowObjectsPerTenant so the cleanup has more to delete")

	require.Containsf(t, refusal, "still removing its temporary index files",
		"the refusal must say the cleanup is what blocks it; got: %s", refusal)
	require.Containsf(t, refusal, className,
		"the refusal must name the collection it is about; got: %s", refusal)
	require.NotContainsf(t, refusal, `shard "`,
		"a hold is collection-wide and names no shard; got: %s", refusal)
	clusterNodes, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
	require.NoError(t, err)
	for _, node := range clusterNodes.Payload.Nodes {
		require.NotContainsf(t, refusal, node.Name,
			"a refusal names no node; got: %s", refusal)
	}

	// The hold has to release. One that did not would wedge every restore on
	// this node until it restarted.
	require.Eventuallyf(t, func() bool {
		var missing *clientbackups.BackupsRestoreNotFound
		return errors.As(restoreClasses(t, backend, unknownBackupID, className), &missing)
	}, 120*time.Second, 250*time.Millisecond,
		"once the cleanup drains the same probe must fall through to 404")
}

// createHoldWindowClass builds the multi-tenant class whose teardown this
// test needs to outlast a probe. A hold is node-local, so one node is enough.
func createHoldWindowClass(t *testing.T, className, propName string) {
	t.Helper()
	helper.CreateClass(t, &models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Vectorizer:         "none",
		Properties: []*models.Property{
			{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
		},
	})
	tenants := make([]*models.Tenant, 0, holdWindowTenants)
	for i := range holdWindowTenants {
		tenants = append(tenants, &models.Tenant{
			Name:           fmt.Sprintf("tenant-%03d", i),
			ActivityStatus: models.TenantActivityStatusHOT,
		})
	}
	helper.CreateTenants(t, className, tenants)
	for _, tenant := range tenants {
		importTenantBodies(t, className, propName, tenant.Name, holdWindowObjectsPerTenant)
	}
}

// importTenantBodies fills one tenant, so the migration has work to do on
// every shard the cleanup will later walk.
func importTenantBodies(t *testing.T, className, propName, tenant string, count int) {
	t.Helper()
	body := "Alpha Bravo Charlie Delta Echo Foxtrot Golf Hotel India Juliett"
	objects := make([]*models.Object, count)
	for i := range objects {
		objects[i] = &models.Object{
			Class:      className,
			ID:         strfmt.UUID(uuid.New().String()),
			Tenant:     tenant,
			Properties: map[string]interface{}{propName: body},
		}
	}
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{Objects: objects})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// awaitReindexTaskTerminal takes the cluster-wide arm out of the answer set,
// so a 422 after it returns is unambiguously the hold.
func awaitReindexTaskTerminal(t *testing.T, restURI, taskID string, timeout time.Duration) {
	t.Helper()
	// A plain loop, not require.Eventuallyf: that one evaluates its message
	// arguments before it polls, so the status it reported would always be
	// the zero value.
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		last = reindexTaskStatus(t, restURI, taskID)
		if !liveReindexStatus(last) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("task %s must reach a terminal status before the probes start; last status %q",
		taskID, last)
}

// pollForHoldRefusal returns "" if the deadline passed with no refusal.
func pollForHoldRefusal(t *testing.T, backend, backupID, className string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var refused *clientbackups.BackupsRestoreUnprocessableEntity
		if errors.As(restoreClasses(t, backend, backupID, className), &refused) {
			return errorResponseMessage(refused.Payload)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

// awaitTrackerDirs blocks until at least want shards carry a tracker dir
// for the migrating property.
//
// Without this the test observes nothing. The cleanup only deletes where a
// unit already wrote a tracker, so a cancel taken the moment the task goes
// live leaves it nothing to do and the hold exists for milliseconds. Tenant
// count alone does not widen that: a shard with nothing to sweep is skipped
// without being loaded, so what has to be waited for is trackers, not tenants.
//
// want is a duration knob, not a correctness one. One tracker already makes
// the sweep do work; the rest are there so the window lasts long enough to
// sample, since each one costs the sweep a shard load and a delete.
func awaitTrackerDirs(t *testing.T, ctx context.Context, container testcontainers.Container,
	className, propName string, want int, timeout time.Duration,
) {
	t.Helper()
	script := fmt.Sprintf(`ls -d /data/%s/*/lsm/.migrations/*_%s* 2>/dev/null | wc -l`,
		strings.ToLower(className), propName)
	deadline := time.Now().Add(timeout)
	last := 0
	for time.Now().Before(deadline) {
		code, reader, err := container.Exec(ctx, []string{"sh", "-c", script})
		require.NoError(t, err)
		require.Zero(t, code)
		out := new(strings.Builder)
		if reader != nil {
			_, _ = io.Copy(out, reader)
		}
		last, _ = strconv.Atoi(strings.TrimSpace(stripExecFrames(out.String())))
		if last >= want {
			t.Logf("%d shards carry a tracker for %q", last, propName)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == 0 {
		t.Fatalf("no shard carried a tracker for %q within %s, so the cleanup would have "+
			"nothing to sweep and this test could observe no hold at all", propName, timeout)
	}
	t.Fatalf("only %d of %d shards carried a tracker for %q within %s; raise "+
		"holdWindowObjectsPerTenant so the migration outlives this poll", last, want, propName, timeout)
}

// stripExecFrames drops the multiplexed-stream header bytes the docker
// exec API prefixes each chunk with, leaving the command's own output.
func stripExecFrames(raw string) string {
	var out strings.Builder
	for _, r := range raw {
		if r >= ' ' || r == '\n' {
			out.WriteRune(r)
		}
	}
	return out.String()
}
