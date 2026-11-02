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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// Test_BackupKeepsFrozenCopy restores a frozen tenant after an unfreeze, a
// re-freeze and a class delete each deleted old cloud copies: the copy the
// backup restores from must survive all three.
func Test_BackupKeepsFrozenCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
	t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

	compose, err := docker.New().
		WithOffloadS3("offloading", "us-west-1").
		WithBackendFilesystem().
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() { _ = compose.Terminate(context.Background()) }()
	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		className = "BackupFrozen"
		tenant    = "Tenant1"
		backend   = "filesystem"
		backupID  = "backup-frozen"
	)
	class := &models.Class{
		Class:              className,
		Vectorizer:         "none",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
	}
	helper.CreateClass(t, class)
	helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})
	ids := []strfmt.UUID{
		"00000000-0000-0000-0000-0000000000b1",
		"00000000-0000-0000-0000-0000000000b2",
	}
	for _, id := range ids {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			ID: id, Class: className, Tenant: tenant, Properties: map[string]interface{}{"name": "kept"},
		}))
	}

	client := helper.Client(t)
	setStatus := func(status string) {
		t.Helper()
		helper.UpdateTenants(t, className, []*models.Tenant{{Name: tenant, ActivityStatus: status}})
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Schema.TenantsGetOne(clschema.NewTenantsGetOneParams().
				WithContext(ctx).WithClassName(className).WithTenantName(tenant), nil)
			require.NoError(c, err)
			assert.Equal(c, status, resp.Payload.ActivityStatus)
		}, 2*time.Minute, 500*time.Millisecond)
	}

	setStatus(models.TenantActivityStatusFROZEN)
	_, err = helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backend, backupID)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil)

	setStatus(models.TenantActivityStatusHOT)
	setStatus(models.TenantActivityStatusFROZEN)
	helper.DeleteClass(t, className)

	_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, backend, backupID, nil, false)
	require.NoError(t, err)
	helper.ExpectBackupEventuallyRestored(t, backupID, backend, nil, helper.WithDeadline(2*time.Minute))
	setStatus(models.TenantActivityStatusHOT)
	for _, id := range ids {
		_, err := helper.TenantObject(t, className, id, tenant)
		require.NoError(t, err, id)
	}
}
