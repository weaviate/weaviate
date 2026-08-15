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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// minCaptureWindowProbes is what separates a run that observed the window from
// one that never saw it.
const minCaptureWindowProbes = 3

// TestReindexRefusedForTheWholeCaptureWindow is journey 3's proof, asked the
// way the submit gate makes it answerable. A migration and a capture of the
// same collection can no longer both be running because the operator asked for
// it: the gate refuses the submission. The only way to reach that state is to
// win the race the gate exists to close, so what this asserts is mutual
// exclusion, not "every submission is refused" — a submission and a capture can
// tie, and a tie is a legitimate outcome.
//
// Whichever way each probe lands, one of two things has to hold at the end:
// the migration never started while the capture was open, or the capture was
// not published as good. Anything else means a backup of a half-migrated shard
// was stored and reported as usable.
func TestReindexRefusedForTheWholeCaptureWindow(t *testing.T) {
	ctx := context.Background()

	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	t.Cleanup(func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		className = "OverlapBackstop_Overlapped"
		propName  = "body"
		backend   = "filesystem"
		backupID  = "overlap-backstop"
	)

	createBodyClass(t, className, propName)
	importBodies(t, className, guardDataset)

	_, err := helper.CreateBackup(t, slowBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "the capture must be admitted: nothing is migrating yet")

	snapshotOf := localBackupSnapshot(t, backend, backupID)
	statusOf := localBackupStatus(t, backend, backupID)

	// Probe for as long as the capture is open, rather than stopping at the
	// first refusal: the claim is about the whole window, not its first instant.
	admitted, probes := probeWholeCaptureWindow(t, restURI, className, propName, statusOf, 10*time.Minute)
	require.GreaterOrEqualf(t, len(probes), minCaptureWindowProbes,
		"vacuous run: only %d submissions landed inside the capture window, so this proves nothing "+
			"about the window — grow guardDataset until the capture stays open for several seconds",
		len(probes))

	captured := awaitBackupTerminal(t, snapshotOf, 10*time.Minute)

	if admitted == "" {
		// Mutual exclusion held outright. Every probe was the gate's own 409,
		// and a capture nothing ran through must publish as good.
		require.Equalf(t, string(entitiesbackup.Success), captured.status,
			"no migration was ever admitted, so nothing can have spanned this capture (reason=%q)",
			captured.errMessage)
		t.Logf("mutual exclusion held: %d submissions inside the window, all refused by the gate", len(probes))
		return
	}

	// A submission tied with the capture. The backstop now owns the outcome.
	startedAt := reindexTaskStartedAt(t, restURI, admitted)
	t.Logf("submission %s tied with the capture: task started %s, capture completed %s",
		admitted, startedAt.Format(time.RFC3339Nano), captured.completedAt.Format(time.RFC3339Nano))

	if startedAt.After(captured.completedAt) {
		// The tie resolved outside the window; the capture was never at risk.
		require.Equalf(t, string(entitiesbackup.Success), captured.status,
			"the migration started after the capture closed, so it cannot have spanned it (reason=%q)",
			captured.errMessage)
		return
	}

	require.Equalf(t, string(entitiesbackup.Failed), captured.status,
		"a migration started inside the capture window, so the capture must not be published as good "+
			"(reason=%q)", captured.errMessage)
	// The per-shard gate can also refuse this backup, and its text also says
	// FAILED, runtime-reindex and the collection. Only the commit-time check
	// says "overlapped this backup", so that is what proves which one fired.
	require.Contains(t, captured.errMessage, entitiesbackup.ErrReindexOverlappedBackup.Error(),
		"the commit-time check has to be what failed this backup; got: %s", captured.errMessage)
	require.Contains(t, captured.errMessage, className,
		"the recorded reason must name the collection; got: %s", captured.errMessage)
}

// TestBackupSucceedsWhenAMigrationRunsOnAnotherCollection is the negative arm.
// Without it the test above passes on a gate that refuses everything and a
// backstop that fails everything.
//
// The migration is started BEFORE the capture, which is the only ordering that
// still reaches this state: the submit gate is node-scoped, so once a capture
// is open no submission is admitted anywhere on the node. The per-shard gate is
// collection-scoped, so the capture of an untouched collection is admitted with
// the migration already live, and the commit-time check must then clear it.
func TestBackupSucceedsWhenAMigrationRunsOnAnotherCollection(t *testing.T) {
	ctx := context.Background()

	compose := startGuardNode(ctx, t)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	t.Cleanup(func() { dumpWeaviateLogs(ctx, t, compose.GetWeaviate().Container(), "weaviate") })

	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)

	const (
		capturedClass  = "OverlapBackstop_Captured"
		migratingClass = "OverlapBackstop_Elsewhere"
		propName       = "body"
		backend        = "filesystem"
		backupID       = "overlap-backstop-clean"
	)

	createBodyClass(t, capturedClass, propName)
	importBodies(t, capturedClass, 2_000)
	createBodyClass(t, migratingClass, propName)
	importBodies(t, migratingClass, guardDataset)

	taskID := submitChangeTokenization(t, restURI, migratingClass, propName, "lowercase")
	reindexhelpers.AwaitReindexLive(t, restURI, taskID, reindexhelpers.WithTimeout(60*time.Second))

	_, err := helper.CreateBackup(t, slowBackupConfig(), capturedClass, backend, backupID)
	require.NoErrorf(t, err,
		"a capture of %s must be admitted while %s migrates: the per-shard gate is collection-scoped",
		capturedClass, migratingClass)

	snapshotOf := localBackupSnapshot(t, backend, backupID)
	captured := awaitBackupTerminal(t, snapshotOf, 10*time.Minute)

	// Judge the window after the verdict: a migration that drained before the
	// capture opened would make the SUCCESS below prove nothing.
	require.Truef(t, liveReindexStatus(reindexTaskStatus(t, restURI, taskID)),
		"the migration on %s must still be live once the capture closed, or the capture never "+
			"overlapped it; grow guardDataset until it outlives the capture", migratingClass)
	require.Equalf(t, string(entitiesbackup.Success), captured.status,
		"a migration on a collection this capture never touched must not fail it (reason=%q)",
		captured.errMessage)
}

// probeWholeCaptureWindow submits for as long as the capture is open and
// returns the first task id that was admitted, if any, plus every probe taken.
func probeWholeCaptureWindow(
	t *testing.T, restURI, collection, property string,
	statusOf func() (string, bool), deadline time.Duration,
) (admitted string, probes []reindexProbe) {
	t.Helper()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		status, ok := statusOf()
		if !ok {
			<-ticker.C
			continue
		}
		if backupTerminal(status) {
			return admitted, probes
		}

		httpStatus, body, ok := tryReindexSubmit(restURI, collection, property, "lowercase")
		if !ok {
			<-ticker.C
			continue
		}
		probes = append(probes, reindexProbe{backupStatus: status, httpStatus: httpStatus, body: body})

		switch {
		case httpStatus == http.StatusConflict:
			require.Truef(t, heldBackupSlotRefusal(httpStatus, body),
				"a submission inside the capture window was refused for a reason that is not the "+
					"submit gate's: %s", body)
		case httpStatus == http.StatusAccepted && admitted == "":
			admitted = taskIDOf(t, body)
		case httpStatus != http.StatusAccepted:
			t.Fatalf("a submission inside the capture window answered %d, which is neither the "+
				"gate's refusal nor an admission: %s", httpStatus, body)
		}
		<-ticker.C
	}
	return admitted, probes
}
