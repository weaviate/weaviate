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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// The switched tenants of a cancelled run serve the target, and the others
// the old tokenization, after a restore and after a restart.
func TestCancelledRunSurvivesBackupRestore(t *testing.T) {
	ctx := context.Background()
	compose, err := reindexhelpers.SingleNodeCompose().WithBackendFilesystem().Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	const className = "ReindexBackup_CancelledRun"
	helper.CreateClass(t, &models.Class{
		Class: className, Vectorizer: "none",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: "text", DataType: []string{"text"}, Tokenization: "word"}},
	})
	helper.CreateTenants(t, className, []*models.Tenant{{Name: "small"}, {Name: "large"}})
	objects := []*models.Object{
		{Class: className, Tenant: "small", Properties: map[string]any{"text": "needle phrase"}},
		{Class: className, Tenant: "small", Properties: map[string]any{"text": "other words"}},
	}
	// Large enough to still build after the small tenant switched.
	for i := 0; i < 60_000; i++ {
		objects = append(objects, &models.Object{Class: className, Tenant: "large", Properties: map[string]any{"text": fmt.Sprintf("bulk %d", i)}})
	}
	for start := 0; start < len(objects); start += 1000 {
		_, err := helper.Client(t).Batch.BatchObjectsCreate(batch.NewBatchObjectsCreateParams().
			WithBody(batch.BatchObjectsCreateBody{Objects: objects[start:min(start+1000, len(objects))]}), nil)
		require.NoError(t, err)
	}

	counts := map[[3]string]int{
		{"small", "bm25", "needle phrase"}: 1, {"small", "bm25", "needle"}: 0,
		{"small", "filter", "needle phrase"}: 1, {"small", "filter", "needle"}: 0,
		{"large", "bm25", "42"}: 1, {"large", "filter", "bulk 42"}: 1, {"large", "filter", "42"}: 1,
	}
	count := func(c [3]string) (int, error) {
		search := fmt.Sprintf(`bm25: {query: %q, properties: ["text"]}`, c[2])
		if c[1] == "filter" {
			search = fmt.Sprintf(`where: {path: ["text"], operator: Equal, valueText: %q}`, c[2])
		}
		resp, err := graphqlhelper.QueryGraphQL(t, nil, "",
			fmt.Sprintf(`{ Get { %s(tenant: %q, %s) { _additional { id } } } }`, className, c[0], search), nil)
		if err != nil {
			return 0, fmt.Errorf("query %v: %w", c, err)
		}
		if len(resp.Errors) > 0 {
			return 0, fmt.Errorf("query %v: %s", c, resp.Errors[0].Message)
		}
		get, _ := resp.Data["Get"].(map[string]any)
		items, _ := get[className].([]any)
		return len(items), nil
	}
	requireCounts := func(phase string) {
		require.EventuallyWithTf(t, func(ct *assert.CollectT) {
			for c, want := range counts {
				got, err := count(c)
				if assert.NoError(ct, err) {
					assert.Equalf(ct, want, got, "%v", c)
				}
			}
		}, time.Minute, 500*time.Millisecond, "exact counts %s", phase)
	}

	restURI := compose.GetWeaviate().URI()
	reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	require.Eventually(t, func() bool {
		phrase, err1 := count([3]string{"small", "bm25", "needle phrase"})
		word, err2 := count([3]string{"small", "bm25", "needle"})
		return err1 == nil && err2 == nil && phrase == 1 && word == 0
	}, 2*time.Minute, 100*time.Millisecond, "the small tenant must switch")
	require.Equal(t, "CANCELLED", reindexhelpers.CancelIndex(t, restURI, className, "text", "searchable").Status)
	requireCounts("after the cancel")

	backupID := backUpOnceTheCancelledScanEnds(t, className)
	helper.DeleteClass(t, className)
	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, "filesystem", backupID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, backupID, "filesystem", nil, helper.WithDeadline(2*time.Minute))
	requireCounts("after the restore")

	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	helper.SetupClient(compose.GetWeaviate().URI())
	requireCounts("after the restore and a restart")
}

// backUpOnceTheCancelledScanEnds retries a backup the halt refuses while the cancelled run's scan winds down.
func backUpOnceTheCancelledScanEnds(t *testing.T, className string) string {
	deadline := time.Now().Add(2 * time.Minute)
	for attempt := 0; ; attempt++ {
		backupID := fmt.Sprintf("reindex-backup-cancelled-run-%d", attempt)
		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, "filesystem", backupID)
		require.NoError(t, err)
		var status, reason string
		require.Eventually(t, func() bool {
			resp, err := helper.CreateBackupStatus(t, "filesystem", backupID, "", "")
			if err != nil || resp.Payload == nil {
				return false
			}
			status, reason = *resp.Payload.Status, resp.Payload.Error
			return status == "SUCCESS" || status == "FAILED"
		}, 2*time.Minute, 500*time.Millisecond, "backup %s never ended", backupID)
		if status == "SUCCESS" {
			return backupID
		}
		require.Contains(t, reason, "a reindex build is scanning", "backup %s failed", backupID)
		require.True(t, time.Now().Before(deadline), "the cancelled run's scan never ended: %s", reason)
		time.Sleep(time.Second)
	}
}
