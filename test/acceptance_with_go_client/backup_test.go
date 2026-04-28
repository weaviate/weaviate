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

package acceptance_with_go_client

import (
	"acceptance_tests_with_client/usage"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	vv8 "github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/backup"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestBackupWithConcurrentDelete(t *testing.T) {
	ctx := t.Context()

	// v5 ---------------------------------------------------------------------

	c5, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	baseName := t.Name()
	tenant := "tenant"
	includeCollections := make([]string, 5)
	for i := range includeCollections {
		collectionName := baseName + fmt.Sprintf("_%d", i)
		c5.Schema().ClassDeleter().WithClassName(collectionName).Do(ctx)
		defer c5.Schema().ClassDeleter().WithClassName(collectionName).Do(ctx) // intended to run after test
		class := &models.Class{
			Class: collectionName,
			Properties: []*models.Property{
				{Name: "num", DataType: schema.DataTypeText.PropString()},
				{Name: "int", DataType: schema.DataTypeInt.PropString()},
			},
			Vectorizer:         "none",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		}
		require.NoError(t, c5.Schema().ClassCreator().WithClass(class).Do(ctx))
		includeCollections[i] = collectionName

		require.NoError(t, c5.Schema().TenantsCreator().WithClassName(class.Class).WithTenants(models.Tenant{Name: tenant}).Do(ctx))
		objs := make([]*models.Object, 100+i)
		for j := range objs {
			objs[j] = &models.Object{
				Class: collectionName,
				Properties: map[string]interface{}{
					"num": string(rune(j)),
					"int": j,
				},
				Tenant: tenant,
			}
		}

		resp, err := c5.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
		require.NoError(t, err)
		for j := range resp {
			require.Nil(t, resp[j].Result.Errors)
		}
	}

	// de-activate tenants to simplify usage stats checking
	for i := range includeCollections {
		require.NoError(t, c5.Schema().TenantsUpdater().WithClassName(includeCollections[i]).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx))
		require.NoError(t, c5.Schema().TenantsUpdater().WithClassName(includeCollections[i]).WithTenants(models.Tenant{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx))
	}

	usageReports := make([]usage.CollectionUsage, len(includeCollections))
	for i := range includeCollections {
		report, err := usage.GetDebugUsageForCollection(includeCollections[i])
		require.NoError(t, err)
		usageReports[i] = *report
	}

	backupID := fmt.Sprintf("concurrent-delete-%016x", rand.Uint64())

	// v6 ---------------------------------------------------------------------

	c6, err := vv8.NewLocal(ctx)
	require.NoError(t, err)
	require.NotNil(t, c6, "v6 client")

	bak, err := c6.Backup.Create(ctx, backup.Create{
		ID:                 backupID,
		Backend:            "filesystem",
		IncludeCollections: includeCollections,
	})
	require.NoError(t, err)

	// Give the backup a moment to start. There are 3 phases in the backup:
	// 1) coordinator - this is done during the Create() call above
	// 2) file listing - this is not yet "delete-safe", so we need to wait to ensure that we are not in this phase anymore
	// 3) actual file copying - this is "delete-safe", so we can delete classes while this is ongoing
	time.Sleep(500 * time.Millisecond)

	for _, collectionName := range includeCollections {
		require.NoError(t, c6.Collections.Delete(ctx, collectionName))
	}

	_, err = backup.AwaitCompletion(ctx, bak)
	require.NoError(t, err)

	restore, err := c6.Backup.Restore(ctx, backup.Restore{
		ID:      bak.ID,
		Backend: bak.Backend,
	})
	require.NoError(t, err)

	_, err = backup.AwaitCompletion(ctx, restore)
	require.NoError(t, err)

	for i, collectionName := range includeCollections {
		exists, err := c6.Collections.Exists(ctx, collectionName)
		require.NoError(t, err, collectionName)
		require.Truef(t, exists, "%q does not exist after restore", collectionName)

		h := c6.Collections.Use(collectionName, collections.WithTenant(tenant))
		count, err := h.Count(ctx)
		require.NoError(t, err)
		require.EqualValuesf(
			t, 100+i, count,
			"%q should have %d objects after restore",
			collectionName, 100+i,
		)

		// v5 --- filter work -------------------------------------------------
		filter := filters.Where()
		filter.WithOperator(filters.LessThan)
		filter.WithValueInt(5)
		filter.WithPath([]string{"int"})

		result, err := c5.GraphQL().Get().WithClassName(collectionName).WithTenant(tenant).
			WithWhere(filter).WithFields(graphql.Field{Name: "int"}).Do(ctx)
		require.NoError(t, err)
		require.Nil(t, result.Errors)

		objects := result.Data["Get"].(map[string]interface{})[collectionName].([]interface{})
		require.Len(t, objects, 5, "class %s should have 5 objects with int < 5 after restore", collectionName)
		// --------------------------------------------------------------------
	}

	// Verify usage stats to ensure no data loss and all data is correctly restored
	for i := range includeCollections {
		report, err := usage.GetDebugUsageForCollection(includeCollections[i])
		require.NoError(t, err)
		require.NoError(t, usage.CollectionUsageDifference(*report, usageReports[i]))
	}

	// v6 ---------------------------------------------------------------------

	// Verify that we can insert new data (needs to be after usage module comparison, because this adds new data)
	for i, collectionName := range includeCollections {
		h := c6.Collections.Use(collectionName, collections.WithTenant(tenant))
		_, err = h.Data.Insert(ctx, &data.Object{
			Properties: map[string]any{"num": string(rune(i))},
		})
		require.NoError(t, err, "insert in %q", collectionName)
	}
}
