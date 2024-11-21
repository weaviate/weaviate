//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/replica"
)

// In this scenario, we are testing two things:
//
//  1. The invocation of shard.UpdateAsyncReplication on an index with inactive tenants,
//     using ForEachLoadedShard to avoid force loading shards for the purpose of enabling
//     async replication, ensuring that if it is enabled when some tenants are inactive,
//     that the change is correctly applied to the tenants if they are later activated
//
//  2. That once (1) occurs, the hashBeat process is correctly initiated at the time that
//     the tenant is activated, and any missing objects are successfully propagated to the
//     nodes which are missing them.
//
// The actual scenario is as follows:
//   - Create a class with multi-tenancy enabled, replicated by a factor of 3
//   - Create a tenant, and set it to inactive
//   - Stop the second node
//   - Activate the tenant, and insert 1000 objects into the 2 surviving nodes
//   - Restart the second node and ensure it has an object count of 0
//   - Update the class to enabled async replication
//   - Wait a few seconds for objects to propagate
//   - Verify that the resurrected node contains all objects
func asyncRepairMultiTenancyScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var (
		clusterSize = 3
		tenantName  = "tenant-0"
		objectCount = 1000
	)

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	paragraphClass := articles.ParagraphsClass()

	t.Run("create schema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:       int64(clusterSize),
			AsyncEnabled: true,
		}
		paragraphClass.Vectorizer = "text2vec-contextionary"
		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
			AutoTenantActivation: true,
			Enabled:              true,
		}

		helper.SetupClient(compose.GetWeaviate().URI())
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("add inactive tenant", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
		helper.CreateTenants(t, paragraphClass.Class, tenants)
	})

	t.Run("stop node 2", func(t *testing.T) {
		stopNodeAt(ctx, t, compose, 2)
	})

	// Activate/insert tenants while node 2 is down
	t.Run("activate tenant and insert paragraphs", func(t *testing.T) {
		batch := make([]*models.Object, objectCount)
		for i := 0; i < objectCount; i++ {
			batch[i] = articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenantName).
				Object()
		}
		createObjectsCL(t, compose.GetWeaviate().URI(), batch, replica.One)
	})

	t.Run("start node 2", func(t *testing.T) {
		startNodeAt(ctx, t, compose, 2)
	})

	t.Run("validate async object propagation", func(t *testing.T) {
		timeout := time.Minute
		start := time.Now()
		for {
			resp := gqlTenantGet(t, compose.GetWeaviateNode(2).URI(), paragraphClass.Class, replica.One, tenantName)
			if len(resp) != objectCount {
				time.Sleep(time.Second)
				continue
			} else if time.Since(start) >= timeout {
				t.Fatalf("expected %d objects, found %d", objectCount, len(resp))
			} else {
				// Test was successful
				break
			}
		}
	})
}
