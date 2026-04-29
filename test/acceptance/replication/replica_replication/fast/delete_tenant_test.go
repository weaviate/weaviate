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

package replication

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func (suite *ReplicationTestSuite) TestReplicationDeletingTenantCleansUpOperations() {
	t := suite.T()

	helper.SetupClient(suite.compose.GetWeaviate().URI())
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
		AutoTenantCreation: true,
		Enabled:            true,
	}

	tenant1 := "tenant1"
	tenant2 := "tenant2"

	helper.DeleteClass(t, paragraphClass.Class)
	helper.CreateClass(t, paragraphClass)

	batch := make([]*models.Object, 0, 1000)
	// 50k objects so HYDRATING's file copy reliably covers the cancel window.
	t.Run(fmt.Sprintf("insert paragraphs into %s", tenant1), func(t *testing.T) {
		for i := 0; i < 50_000; i++ {
			batch = append(batch, articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenant1).
				Object())
			if i%1_000 == 0 {
				helper.CreateObjectsBatch(t, batch)
				fmt.Printf("created %d objects for %s\n", i, tenant1)
				batch = batch[:0]
			}
		}
		helper.CreateObjectsBatch(t, batch)
	})

	// tenant2 is the unaffected-op control; its op runs to READY untouched.
	t.Run(fmt.Sprintf("insert paragraphs into %s", tenant2), func(t *testing.T) {
		for i := 0; i < 5_000; i++ {
			batch = append(batch, articles.NewParagraph().
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				WithTenant(tenant2).
				Object())
			if i%1_000 == 0 {
				helper.CreateObjectsBatch(t, batch)
				fmt.Printf("created %d objects for %s\n", i, tenant2)
				batch = batch[:0]
			}
		}
		helper.CreateObjectsBatch(t, batch)
	})

	// Keep the change-capture log non-empty during the op so cancelOp's
	// StopChangeCapture runs against an active log, not a quiet shard.
	// Writes after DeleteTenants(tenant1) fail by design — ignored.
	logger, _ := logrustest.NewNullLogger()
	parallelWriteWg := sync.WaitGroup{}
	parallelWriteDone := make(chan struct{})
	parallelWriteWg.Add(1)
	enterrors.GoWrapper(func() {
		defer parallelWriteWg.Done()
		containerId := 1
		clusterSize := 3
		for {
			select {
			case <-parallelWriteDone:
				return
			default:
				_ = createObjectThreadSafe(
					suite.compose.ContainerURI(containerId),
					paragraphClass.Class,
					map[string]interface{}{"contents": "parallel-write"},
					uuid.New().String(),
					tenant1,
				)
				containerId++
				if containerId > clusterSize {
					containerId = 1
				}
			}
		}
	}, logger)
	defer func() {
		close(parallelWriteDone)
		parallelWriteWg.Wait()
	}()

	var id1 strfmt.UUID
	t.Run(fmt.Sprintf("create replication operation for %s", tenant1), func(t *testing.T) {
		created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getMTRequest(t, paragraphClass.Class, tenant1)), nil)
		require.Nil(t, err)
		require.NotNil(t, created)
		require.NotNil(t, created.Payload)
		require.NotNil(t, created.Payload.ID)
		id1 = *created.Payload.ID
	})

	var id2 strfmt.UUID
	t.Run(fmt.Sprintf("create replication operation for %s", tenant2), func(t *testing.T) {
		created, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(getMTRequest(t, paragraphClass.Class, tenant2)), nil)
		require.Nil(t, err)
		require.NotNil(t, created)
		require.NotNil(t, created.Payload)
		require.NotNil(t, created.Payload.ID)
		id2 = *created.Payload.ID
	})

	// Leaving REGISTERED is enough to guarantee the worker has dispatched —
	// pinning a specific state is fragile now that FINALIZING is <50ms.
	t.Run(fmt.Sprintf("wait until op for %s leaves REGISTERED", tenant1), func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id1), nil)
			require.Nil(ct, err)
			require.NotEqual(ct, api.REGISTERED.String(), details.Payload.Status.State, "op should have left REGISTERED")
		}, 10*time.Second, 100*time.Millisecond, "op for %s never left REGISTERED", tenant1)
	})

	t.Run(fmt.Sprintf("delete %s", tenant1), func(t *testing.T) {
		helper.DeleteTenants(t, paragraphClass.Class, []string{tenant1})
	})

	t.Run("wait for replication operation to be deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id1), nil)
			require.NotNil(ct, err)
			assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
		}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
	})

	t.Run(fmt.Sprintf("ensure that the replication operation for %s is still there", tenant2), func(t *testing.T) {
		details, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id2), nil)
		require.Nil(t, err)
		require.NotNil(t, details)
		require.NotNil(t, details.Payload)
		require.NotNil(t, details.Payload.ID)
		require.Equal(t, id2, *details.Payload.ID)
	})

	t.Run(fmt.Sprintf("delete %s", tenant2), func(t *testing.T) {
		helper.DeleteTenants(t, paragraphClass.Class, []string{tenant2})
	})

	t.Run("wait for replication operation to be deleted", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(id2), nil)
			require.NotNil(ct, err)
			assert.IsType(ct, replication.NewReplicationDetailsNotFound(), err)
		}, 30*time.Second, 1*time.Second, "replication operation should be deleted")
	})
}

func getMTRequest(t *testing.T, className, tenant string) *models.ReplicationReplicateReplicaRequest {
	verbosity := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbosity).WithClassName(className), nil)
	require.Nil(t, err)

	// find node with tenant
	sourceNode := ""
	for _, node := range nodes.Payload.Nodes {
		for _, shard := range node.Shards {
			if shard.Name == tenant {
				sourceNode = node.Name
				break
			}
		}
	}

	// find node without tenant
	destinationNode := ""
	for _, node := range nodes.Payload.Nodes {
		shards := make([]string, 0)
		for _, shard := range node.Shards {
			shards = append(shards, shard.Name)
		}
		if sourceNode != node.Name && !slices.Contains(shards, tenant) {
			destinationNode = node.Name
			break
		}
	}

	return &models.ReplicationReplicateReplicaRequest{
		Collection: &className,
		SourceNode: &sourceNode,
		TargetNode: &destinationNode,
		Shard:      &tenant,
	}
}
