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

package usage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"golang.org/x/sync/errgroup"
)

func TestTenantStatusChanges(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := t.Name() + "Class"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 10)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	// add some data
	for i, tenant := range tenants {
		_, err := c.Data().Creator().WithClassName(className).
			WithTenant(tenant.Name).
			WithProperties(map[string]interface{}{
				"first": fmt.Sprintf("hello%d", i),
			}).Do(ctx)
		require.NoError(t, err)
	}
	endUsage := atomic.Bool{}

	go func() {
		for {
			if endUsage.Load() {
				return
			}

			usage, err := getDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			require.Equal(t, len(usage.Shards), len(tenants))

			require.NotNil(t, usage.UniqueShardCount)
			require.Equal(t, len(usage.Shards), *usage.UniqueShardCount)

			names := make(map[string]struct{})
			for _, shard := range usage.Shards {
				require.NotNil(t, shard.Name)
				if _, ok := names[*shard.Name]; ok {
					require.Fail(t, "duplicate shard name found")
				}
				names[*shard.Name] = struct{}{}
			}
			require.Equal(t, len(names), len(tenants))
		}
	}()

	var eg errgroup.Group
	for i := range tenants {
		eg.Go(
			func() error {
				require.NoError(t,
					c.Schema().TenantsUpdater().WithClassName(className).WithTenants(models.Tenant{Name: fmt.Sprintf("tenant%d", i), ActivityStatus: models.TenantActivityStatusCOLD}).Do(ctx),
				)
				require.NoError(t,
					c.Schema().TenantsUpdater().WithClassName(className).WithTenants(models.Tenant{Name: fmt.Sprintf("tenant%d", i), ActivityStatus: models.TenantActivityStatusHOT}).Do(ctx),
				)
				return nil
			},
		)
	}
	require.NoError(t, eg.Wait())
	endUsage.Store(true)
}

func TestUsageTenantDelete(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := t.Name() + "Class"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 100)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	// add some data
	for i, tenant := range tenants {
		_, err := c.Data().Creator().WithClassName(className).
			WithTenant(tenant.Name).
			WithProperties(map[string]interface{}{
				"first": fmt.Sprintf("hello%d", i),
			}).Do(ctx)
		require.NoError(t, err)
	}

	endUsage := atomic.Bool{}
	deletedTenants := atomic.Int32{}
	go func() {
		for {
			if endUsage.Load() {
				return
			}
			deletedTenantsBeforeCall := deletedTenants.Load()
			usage, err := getDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			deletedTenantsAfterCall := deletedTenants.Load()

			// we add a bit of wiggle room here as the usage endpoint might take a bit to reflect the changes
			require.LessOrEqual(t, len(usage.Shards), len(tenants)-int(deletedTenantsBeforeCall)+1)
			require.GreaterOrEqual(t, len(usage.Shards), len(tenants)-int(deletedTenantsAfterCall)-1)

			if len(usage.Shards) > 0 {
				require.NotNil(t, usage.UniqueShardCount)
				require.Equal(t, len(usage.Shards), *usage.UniqueShardCount)
			}

			names := make(map[string]struct{})
			for _, shard := range usage.Shards {
				require.NotNil(t, shard.Name)
				if _, ok := names[*shard.Name]; ok {
					require.Fail(t, "duplicate shard name found")
				}
				names[*shard.Name] = struct{}{}
			}
		}
	}()

	for i := range tenants {
		err := c.Schema().TenantsDeleter().WithClassName(className).WithTenants(tenants[i].Name).Do(ctx)
		require.NoError(t, err)
		deletedTenants.Add(1)
	}
	endUsage.Store(true)
}

func TestCollectionDeletion(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	getClassName := func(t *testing.T, i int) string {
		return t.Name() + "Class" + fmt.Sprintf("%d", i)
	}
	numClasses := 100

	c.Schema().AllDeleter().Do(ctx)
	classCreator := c.Schema().ClassCreator()
	// create a bunch of classes
	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)

		c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "first",
					DataType: []string{string(schema.DataTypeText)},
				},
			},
		}
		require.NoError(t, classCreator.WithClass(class).Do(ctx))
	}

	endUsage := atomic.Bool{}
	deletedClasses := atomic.Int32{}
	go func() {
		for {
			if endUsage.Load() {
				return
			}
			deletedClassesBeforeCall := deletedClasses.Load()
			usage, err := getDebugUsage()
			require.NoError(t, err)
			require.NotNil(t, usage)
			deletedClassesAfterCall := deletedClasses.Load()

			// we add a bit of wiggle room here as the usage endpoint might take a bit to reflect the changes
			require.LessOrEqual(t, len(usage.Collections), numClasses-int(deletedClassesBeforeCall)+1)
			require.GreaterOrEqual(t, len(usage.Collections), numClasses-int(deletedClassesAfterCall)-1)
		}
	}()

	for i := 0; i < numClasses; i++ {
		className := getClassName(t, i)
		require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))
		deletedClasses.Add(1)
	}
	endUsage.Store(true)
}

func TestRestart(t *testing.T) {
	t.Skip("debug endpoint needs to be available for compose")
	ctx := context.Background()

	//compose, err := docker.New().
	//	WithWeaviate().
	//	WithWeaviateEnv("TRACK_VECTOR_DIMENSIONS", "true").
	//	Start(ctx)
	//require.NoError(t, err)
	//defer func() {
	//	require.NoError(t, compose.Terminate(ctx))
	//}()

	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	className := t.Name() + "Class"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	tenants := make([]models.Tenant, 100)
	for i := range tenants {
		tenants[i] = models.Tenant{Name: fmt.Sprintf("tenant%d", i)}
	}
	require.NoError(t, c.Schema().TenantsCreator().WithClassName(className).WithTenants(tenants...).Do(ctx))

	// add some data
	for i, tenant := range tenants {
		objs := make([]*models.Object, 10)
		for j := range objs {
			vector := make([]float32, 128)
			for k := range vector {
				vector[k] = float32(i+j+k) / 10000.0
			}
			objs[j] = &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"first": fmt.Sprintf("hello%d-%d", i, j),
				},
				Vector: vector,
				Tenant: tenant.Name,
			}
		}

		_, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
		require.NoError(t, err)
	}

	usage, err := getDebugUsage()
	require.NoError(t, err)
	require.NotNil(t, usage)

	// timeout := time.Minute
	// require.NoError(t, compose.StopAt(ctx, 0, &timeout))
	// require.NoError(t, compose.StartAt(ctx, 0))

	usage2, err := getDebugUsage()
	require.NoError(t, err)
	require.NotNil(t, usage2)
	require.NoError(t, ReportsDifference(usage, usage2))
}
