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

			usage, err := GetDebugUsageForCollection(className)
			require.NoError(t, err)
			require.NotNil(t, usage)
			require.Equal(t, len(usage.Shards), len(tenants))

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
