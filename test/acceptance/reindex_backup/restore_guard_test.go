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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientbackups "github.com/weaviate/weaviate/client/backups"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// Without the cross-collection restore below, a gate refusing every restore would still pass this test.
func testRestoreRefusedDuringInFlightReindex(t *testing.T, restURI string) {
	helper.SetupClient(restURI)
	const (
		unrelatedClass = "RestoreGuard_Payload"
		reindexClass   = "RestoreGuard_Migrating"
		backend        = "filesystem"
		backupID       = "restore-guard-refuse"
	)
	createBodyClass(t, unrelatedClass, "body")
	importBodies(t, unrelatedClass, 200)
	createBodyClass(t, reindexClass, "body")
	importBodies(t, reindexClass, guardDataset)

	createBackupOf(t, backend, backupID, unrelatedClass, reindexClass)

	// Deleted after the backup so its restore is otherwise valid.
	helper.DeleteClass(t, unrelatedClass)
	taskID := submitChangeTokenization(t, restURI, reindexClass, "body", "lowercase")
	t.Logf("change-tokenization task submitted: %s", taskID)
	reindexhelpers.AwaitReindexLive(t, restURI, taskID,
		reindexhelpers.WithTimeout(30*time.Second))
	statusBefore := reindexTaskStatus(t, restURI, taskID)
	sameCollectionErr := restoreClasses(t, backend, backupID, unrelatedClass, reindexClass)
	crossCollectionErr := restoreClasses(t, backend, backupID, unrelatedClass)
	statusAfter := reindexTaskStatus(t, restURI, taskID)

	require.Truef(t, liveReindexStatus(statusBefore) && liveReindexStatus(statusAfter),
		"the migration must still be live on both sides of the two restore calls "+
			"(before=%q after=%q); grow guardDataset until it outlives them",
		statusBefore, statusAfter)

	require.Error(t, sameCollectionErr,
		"a restore including the migrating collection must be refused synchronously")
	var refusal *clientbackups.BackupsRestoreUnprocessableEntity
	require.ErrorAsf(t, sameCollectionErr, &refusal,
		"expected 422 BackupsRestoreUnprocessableEntity, got %T: %v",
		sameCollectionErr, sameCollectionErr)
	require.NotNil(t, refusal.Payload, "422 payload must not be nil")
	errMsg := errorResponseMessage(refusal.Payload)
	require.Contains(t, errMsg, "restore blocked: runtime-reindex in flight in the cluster",
		"the body must name the refused operation and the blocking condition; got: %s", errMsg)
	require.Contains(t, errMsg, "retry after the migration finishes",
		"the body must include an actionable next step; got: %s", errMsg)
	require.Contains(t, errMsg, reindexClass,
		"the body must name the collection that is blocked; got: %s", errMsg)
	// "backup blocked" is the backup gate's prefix; seeing it here means the two gates got crossed.
	require.NotContains(t, errMsg, "backup blocked",
		"a restore refusal must not be worded as a backup refusal; got: %s", errMsg)
	requireNoPlacement(t, errMsg, reindexhelpers.GetFirstShardName(t, restURI, reindexClass))

	_, exists := reindexhelpers.FetchClass(restURI, unrelatedClass, true)
	require.Falsef(t, exists, "a refused restore must not create %s", unrelatedClass)

	require.NoErrorf(t, crossCollectionErr,
		"a restore of %s must be admitted while the migration on %s is live",
		unrelatedClass, reindexClass)
	helper.ExpectBackupEventuallyRestored(t, backupID, backend, nil,
		helper.WithDeadline(2*time.Minute))
	_, exists = reindexhelpers.FetchClass(restURI, unrelatedClass, true)
	require.Truef(t, exists, "the admitted restore must have created %s", unrelatedClass)
}
