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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// TestGatesAreNoOpsWithTheFeatureOff runs the shipped default: with
// RUNTIME_REINDEX_ENABLED off every gate returns before consulting
// anything, so a backup and a restore the gates would refuse both succeed.
func TestGatesAreNoOpsWithTheFeatureOff(t *testing.T) {
	ctx := context.Background()
	compose, err := reindexhelpers.SingleNodeComposeWithReindex(false).
		WithBackendFilesystem().
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })
	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)
	t.Cleanup(helper.ResetClient)
	const (
		className = "FlagOff_Class"
		backend   = "filesystem"
		backupID  = "flag-off-noop"
	)
	createBodyClass(t, className, "body")
	importBodies(t, className, 200)
	requireReindexRefused(t, restURI, className)
	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backend, backupID)
	require.NoError(t, err, "with the feature off a backup must behave as it did before the gates existed")
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil,
		helper.WithDeadline(2*time.Minute))
	helper.DeleteClass(t, className)
	require.NoError(t, restoreClasses(t, backend, backupID, className),
		"with the feature off a restore must not be gated either")
	helper.ExpectBackupEventuallyRestored(t, backupID, backend, nil,
		helper.WithDeadline(2*time.Minute))
}

// requireReindexRefused proves the flag really is off, so the successes
// above are the kill switch working rather than a cluster with nothing
// migrating.
func requireReindexRefused(t *testing.T, restURI, className string) {
	t.Helper()
	resp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, className, "body", "searchable",
		`{"tokenization":"lowercase"}`)
	require.Equal(t, 400, resp.StatusCode,
		"a submission must be refused with the feature off; got %d: %s", resp.StatusCode, resp.Body)
	require.Contains(t, resp.Body, "RUNTIME_REINDEX_ENABLED")
}
