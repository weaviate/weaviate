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
	"acceptance_tests_with_client/internal/wvhost"
	"acceptance_tests_with_client/usage"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6/backup"
	"github.com/weaviate/weaviate-go-client/v6/batch"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate-go-client/v6/modules/selfprovided"
	"github.com/weaviate/weaviate-go-client/v6/query"
	"github.com/weaviate/weaviate-go-client/v6/query/filter"
	"github.com/weaviate/weaviate-go-client/v6/tenant"
)

func TestBackupWithConcurrentDelete(t *testing.T) {
	c := wvhost.NewClient(t)

	tenantName := "john_doe"
	collectionNames := make([]string, 5)
	for i := range collectionNames {
		collectionName := t.Name() + fmt.Sprintf("_%d", i)
		collectionNames[i] = collectionName

		require.NoError(t, c.Collections.Delete(ctx, collectionName))
		t.Cleanup(func() {
			require.NoError(t, c.Collections.Delete(ctx, collectionName))
		})

		h, err := c.Collections.Create(ctx, collections.Collection{
			Name: collectionName,
			Properties: []collections.Property{
				{Name: "num", DataType: collections.DataTypeText},
				{Name: "int", DataType: collections.DataTypeInt},
				{
					Name:     "cars",
					DataType: collections.DataTypeObjectArray,
					NestedProperties: []collections.Property{
						{
							Name:            "make",
							DataType:        collections.DataTypeText,
							Tokenization:    collections.TokenizationField,
							IndexFilterable: true,
						},
					},
				},
			},
			Vectors: map[string]collections.VectorConfig{
				"default": {Vectorizer: selfprovided.Vectorizer},
			},
			MultiTenancy: &collections.MultiTenancyConfig{Enabled: true},
		})
		require.NoError(t, err)
		require.NotNil(t, h, "collection handle")

		require.NoError(t, h.Tenants.Create(ctx, tenant.Tenant{Name: tenantName}))

		h = h.WithOptions(collections.WithTenant(tenantName))
		b := h.Batch(ctx)
		var tasks []*batch.Task
		for j := range 100 + i {
			// Two cars per object: cars[0]=Toyota for even j, Honda for odd j.
			cars := []string{"Toyota", "Honda"}
			if j%2 == 1 {
				cars[0], cars[1] = cars[1], cars[0]
			}
			task, err := b.Object(ctx, &data.Object{
				Properties: map[string]any{
					"num": string(rune(j)),
					"int": j,
					"cars": []any{
						map[string]any{"make": cars[0]},
						map[string]any{"make": cars[1]},
					},
				},
			})
			require.NoError(t, err, "add object to batch (collection=%s tenant=%s)", h.CollectionName(), h.Tenant())
			tasks = append(tasks, task)
		}
		require.NoError(t, b.Close(), "batch failed")

		// This is required to catch any object-level errors, b.Close doesn't report them.
		for _, task := range tasks {
			require.NoError(t, task.Wait(), "wait for object to get inserted")
		}

		count, err := h.Count(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 100+i, count, "wrong number of objects after BATCH in %q", h.CollectionName())
	}

	// De-activate tenants to simplify usage stats checking.
	for i := range collectionNames {
		h := c.Collections.Use(collectionNames[i])
		require.NoError(t, h.Tenants.Update(ctx, tenant.Tenant{Name: tenantName, Status: tenant.Cold}), "set %s=%s", tenantName, tenant.Cold)
		require.NoError(t, h.Tenants.Update(ctx, tenant.Tenant{Name: tenantName, Status: tenant.Hot}), "set %s=%s", tenantName, tenant.Hot)
	}

	usageReports := make([]usage.CollectionUsage, len(collectionNames))
	for i := range usageReports {
		report, err := usage.GetDebugUsageForCollection(collectionNames[i])
		require.NoError(t, err)
		usageReports[i] = *report
	}

	backupID := fmt.Sprintf("concurrent-delete-%016x", rand.Uint64())
	bak, err := c.Backup.Create(ctx, backup.CreateOptions{
		ID:                 backupID,
		Backend:            "filesystem",
		IncludeCollections: collectionNames,
	})
	require.NoError(t, err)
	require.NotNil(t, bak, "backup info for %q", backupID)

	// give the backup a moment to start. There are 3 phases in the backup:
	// 1) coordinator - this is done during the Creator() call above
	// 2) file listing - this is not yet "delete-safe", so we need to wait to ensure that we are not in this phase anymore
	// 3) actual file copying - this is "delete-safe", so we can delete classes while this is ongoing
	time.Sleep(500 * time.Millisecond)
	for _, name := range collectionNames {
		require.NoError(t, c.Collections.Delete(ctx, name))
	}

	if _, err := backup.AwaitCompletion(ctx, bak); err != nil {
		t.Fatalf("backup %q failed: %v", bak.ID, err)
	}

	restore, err := c.Backup.Restore(ctx, backup.RestoreOptions{
		ID:      bak.ID,
		Backend: bak.Backend,
	})
	require.NoError(t, err)
	require.NotNil(t, restore, "backup restore info for %q", backupID)

	if _, err := backup.AwaitCompletion(ctx, restore); err != nil {
		t.Fatalf("backup %s restore failed: %v", bak.ID, err)
	}

	for i, name := range collectionNames {
		wantObjects := 100 + i

		exists, err := c.Collections.Exists(ctx, name)
		require.NoError(t, err)
		require.True(t, exists, "collection %s should exist after restore", name)

		h := c.Collections.Use(name, collections.WithTenant(tenantName))
		count, err := h.Count(ctx)
		require.NoError(t, err)
		require.EqualValues(t, wantObjects, count, "wrong number of objects after restore in %q", name)

		// filter work
		res, err := h.Query.OverAll(ctx, query.OverAll{
			Filter: filter.Cond{
				Target:   "int",
				Operator: filter.LessThan,
				Value:    5,
			},
			Limit: wantObjects,
		})
		require.NoError(t, err)
		require.NotNil(t, res, "query result")
		require.Len(t, res.Objects, 5, "collection %q should have 5 objects with `int` < 5 after restore", name)

		// Verify that both 1) the nested filterable value and 2) the meta bucket
		// were included in the backup and rebuilt on restore.

		// 1) Each object's cars.make contains a "Toyota" entry.
		res, err = h.Query.OverAll(ctx, query.OverAll{
			Filter: filter.Cond{
				Target:   "cars.make",
				Operator: filter.Equal,
				Value:    "Toyota",
			},
			Limit: wantObjects,
		})
		require.NoError(t, err)
		require.NotNil(t, res, "query result")
		require.Len(t, res.Objects, wantObjects, "collection %q: cars.make=Toyota must match all objects after restore", name)

		// 2) Only half of the objects has "Toyota" at index 0 in cars.make.
		res, err = h.Query.OverAll(ctx, query.OverAll{
			Filter: filter.Cond{
				Target:   "cars[0].make",
				Operator: filter.Equal,
				Value:    "Toyota",
			},
			Limit: wantObjects,
		})
		require.NoError(t, err)
		require.NotNil(t, res, "query result")
		half := (wantObjects + 1) / 2
		require.Len(t, res.Objects, half, "collection %q: cars[0].make=Toyota must match %d objects after restore", name, half)
	}

	// Verify usage stats to ensure no data loss and all data is correctly restored
	for i := range collectionNames {
		report, err := usage.GetDebugUsageForCollection(collectionNames[i])
		require.NoError(t, err)
		require.NoError(t, usage.CollectionUsageDifference(*report, usageReports[i]))
	}

	// verify that we can insert new data (needs to be after usage module comparison, because this adds new data)
	for i, name := range collectionNames {
		h := c.Collections.Use(name, collections.WithTenant(tenantName))
		b := h.Batch(ctx)
		for range i + 1 {
			_, err := b.Object(ctx, &data.Object{
				Properties: map[string]any{
					"num": string(rune(i)),
				},
			})
			require.NoError(t, err, "add object to batch")
		}
		require.NoError(t, b.Close(), "batch failed")
	}
}
