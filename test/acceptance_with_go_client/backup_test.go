//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"acceptance_tests_with_client/usage"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestBackupWithConcurrentDelete(t *testing.T) {
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	baseName := t.Name()
	numClasses := 5
	tenant := "tenant"
	classNames := make([]string, numClasses)
	for i := range numClasses {
		className := baseName + fmt.Sprintf("_%d", i)
		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
		defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx) // intended to run after test
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "num", DataType: schema.DataTypeText.PropString()},
				{Name: "int", DataType: schema.DataTypeInt.PropString()},
			},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
		classNames[i] = className

		require.NoError(t, c.Schema().TenantsCreator().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant}).Do(ctx))
		objs := make([]*models.Object, 100+i)
		for j := range objs {
			objs[j] = &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"num": string(rune(j)),
					"int": j,
				},
				Tenant: tenant,
			}
		}

		resp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
		require.NoError(t, err)
		for j := range resp {
			require.Nil(t, resp[j].Result.Errors)
		}
	}

	// de-activate tenants to simplify usage stats checking
	for i := range classNames {
		require.NoError(t, c.Schema().TenantsUpdater().WithClassName(classNames[i]).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))
		require.NoError(t, c.Schema().TenantsUpdater().WithClassName(classNames[i]).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx))
	}

	usageReports := make([]usage.CollectionUsage, numClasses)
	for i := range classNames {
		report, err := usage.GetDebugUsageForCollection(classNames[i])
		require.NoError(t, err)
		usageReports[i] = *report
	}

	backupID := fmt.Sprintf("concurrent-delete-%016x", rand.Uint64())

	_, err = c.Backup().Creator().WithBackupID(backupID).WithBackend("filesystem").WithIncludeClassNames(classNames...).Do(ctx)
	require.NoError(t, err)

	// give the backup a moment to start. There are 3 phases in the backup:
	// 1) coordinator - this is done during the Creator() call above
	// 2) file listing - this is not yet "delete-safe", so we need to wait to ensure that we are not in this phase anymore
	// 3) actual file copying - this is "delete-safe", so we can delete classes while this is ongoing
	time.Sleep(500 * time.Millisecond)
	for _, name := range classNames {
		require.NoError(t, c.Schema().ClassDeleter().WithClassName(name).Do(ctx))
	}

	for {
		status, err := c.Backup().CreateStatusGetter().WithBackupID(backupID).WithBackend("filesystem").Do(ctx)
		require.NoError(t, err)

		if *status.Status == models.BackupCreateResponseStatusSUCCESS {
			break
		}
		if *status.Status == models.BackupCreateResponseStatusFAILED {
			t.Fatalf("backup failed: %v", status.Error)
		}
	}

	// backup completed successfully, now verify that all classes were backed up
	_, err = c.Backup().Restorer().WithBackupID(backupID).WithBackend("filesystem").Do(ctx)
	require.NoError(t, err)
	for {
		status, err := c.Backup().RestoreStatusGetter().WithBackupID(backupID).WithBackend("filesystem").Do(ctx)
		require.NoError(t, err)

		if *status.Status == models.BackupRestoreResponseStatusSUCCESS {
			break
		}
		if *status.Status == models.BackupRestoreResponseStatusFAILED {
			t.Fatalf("restore failed: %v", status.Error)
		}
	}

	for i, name := range classNames {
		exists, err := c.Schema().ClassExistenceChecker().WithClassName(name).Do(ctx)
		require.NoError(t, err)
		require.True(t, exists, "class %s should exist after restore", name)

		data, err := c.GraphQL().Aggregate().WithClassName(name).WithTenant(tenant).WithFields(graphql.Field{
			Name: "meta",
			Fields: []graphql.Field{
				{Name: "count"},
			},
		}).Do(ctx)
		require.NoError(t, err)

		classData := data.Data["Aggregate"].(map[string]interface{})[name].([]interface{})[0].(map[string]interface{})
		count := classData["meta"].(map[string]interface{})["count"].(float64)
		expectedCount := float64(100 + i)
		require.Equal(t, expectedCount, count, "class %s should have %d objects after restore", name, int(expectedCount))

		// filter work
		filter := filters.Where()
		filter.WithOperator(filters.LessThan)
		filter.WithValueInt(5)
		filter.WithPath([]string{"int"})

		result, err := c.GraphQL().Get().WithClassName(name).WithTenant(tenant).
			WithWhere(filter).WithFields(graphql.Field{Name: "int"}).Do(ctx)
		require.NoError(t, err)
		require.Nil(t, result.Errors)

		objects := result.Data["Get"].(map[string]interface{})[name].([]interface{})
		require.Len(t, objects, 5, "class %s should have 5 objects with int < 5 after restore", name)
	}

	// verify usage stats to ensure no data loss and all data is correctly restored
	for i := range classNames {
		report, err := usage.GetDebugUsageForCollection(classNames[i])
		require.NoError(t, err)
		require.NoError(t, usage.CollectionUsageDifference(*report, usageReports[i]))
	}

	// verify that we can insert new data (needs to be after usage module comparison, becuse this adds new data)
	for i, name := range classNames {
		// can insert more data
		objs := make([]*models.Object, 1+i)
		for i := range objs {
			objs[i] = &models.Object{
				Class: name,
				Properties: map[string]interface{}{
					"num": string(rune(i)),
				},
				Tenant: tenant,
			}
		}

		resp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
		require.NoError(t, err)
		for i := range resp {
			require.Nil(t, resp[i].Result.Errors)
		}
	}
}
