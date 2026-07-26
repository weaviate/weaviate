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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

var projectionRepairIDs = []strfmt.UUID{
	strfmt.UUID("6ad5b7dc-5b0e-4d64-9d5b-3f4a3f5f9c01"),
	strfmt.UUID("2c1d1c9f-9d0e-4a9d-9a1a-6c1e2d3f4a02"),
	strfmt.UUID("f0a1b2c3-d4e5-4f60-8a7b-9c0d1e2f3a03"),
	strfmt.UUID("11223344-5566-4778-899a-bbccddeeff04"),
	strfmt.UUID("55667788-99aa-4bcc-8dee-ff0011223305"),
	strfmt.UUID("99aabbcc-ddee-4f00-9112-233445566706"),
}

// The first rewrittenCount objects get rewritten while the replica is down;
// the rest stay untouched so the triggering filter matches a strict subset.
const rewrittenCount = 3

func projectionContents(i int, rewritten bool) string {
	if rewritten {
		return fmt.Sprintf("rewritten%d", i)
	}
	return fmt.Sprintf("original%d", i)
}

func projectionTitle(i int) string { return fmt.Sprintf("title%d", i) }

func projectionVector(i int) []float32 {
	return []float32{float32(i) + 0.25, float32(i) + 0.5, float32(i) + 0.75, 1}
}

// TestReadRepairPreservesProjectedAwayContent pins the regression where read
// repair overwrote a lagging replica with a search result's projection,
// destroying properties and the vector outside the query's selection.
//
// All three parts of the trigger are load-bearing: an unfiltered read carries a
// complete object, a wide selection set leaves nothing to destroy, and
// consistency level ONE short-circuits the repair path before it runs.
func (suite *ReplicationTestSuite) TestReadRepairPreservesProjectedAwayContent() {
	t := suite.T()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		// Vectors are explicit; contextionary only keeps container indices
		// aligned with the rest of the package (slot 0).
		WithText2VecContextionary().
		// Keeps the lagging replica's divergence from healing via async replication.
		// Only on|enabled|1|true engage this flag; "disabled" would silently do nothing.
		WithWeaviateEnv("ASYNC_REPLICATION_DISABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	// Funcs, not vars: restarting a container remaps its published port. The
	// reader must stay up, since repair only overwrites via the winning local copy.
	readerURI := func() string { return compose.ContainerURI(1) }
	laggingURI := func() string { return compose.ContainerURI(3) }
	laggingNode := docker.Weaviate2
	t.Logf("reader node (stays up): %s at %s", docker.Weaviate0, readerURI())
	t.Logf("lagging node (stopped during rewrite): %s at %s", laggingNode, laggingURI())

	helper.SetupClient(readerURI())
	paragraphClass := articles.ParagraphsClass()
	className := paragraphClass.Class

	t.Run("CreateSchema", func(t *testing.T) {
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
			Factor:           3,
			AsyncEnabled:     false,
			DeletionStrategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
		}
		helper.CreateClass(t, paragraphClass)

		got := helper.GetClass(t, className)
		require.EqualValues(t, 3, got.ReplicationConfig.Factor)
		require.False(t, got.ReplicationConfig.AsyncEnabled,
			"async replication must stay off, it would converge the lagging replica and delete the precondition")
	})

	t.Run("InsertOnAllReplicas", func(t *testing.T) {
		batch := make([]*models.Object, len(projectionRepairIDs))
		for i, id := range projectionRepairIDs {
			batch[i] = &models.Object{
				Class: className,
				ID:    id,
				Properties: map[string]interface{}{
					"contents": projectionContents(i, false),
					"title":    projectionTitle(i),
				},
				Vector: projectionVector(i),
			}
		}
		common.CreateObjectsCL(t, readerURI(), batch, types.ConsistencyLevelAll)

		// Confirm full replication before treating later state as a divergence.
		for _, nodeName := range []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2} {
			for i, id := range projectionRepairIDs {
				require.EventuallyWithT(t, func(ct *assert.CollectT) {
					obj, err := common.GetObjectFromNodeWithVector(t, readerURI(), className, id, nodeName)
					if !assert.NoError(ct, err) {
						return
					}
					assertProjectionObject(ct, obj, i, false, nodeName)
				}, 60*time.Second, 500*time.Millisecond,
					"object %s never fully replicated to %s", id, nodeName)
			}
		}
	})

	t.Run("StopLaggingNode", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 3)
	})

	t.Run("RewriteSubsetAtQuorum", func(t *testing.T) {
		for i := 0; i < rewrittenCount; i++ {
			obj := &models.Object{
				Class: className,
				ID:    projectionRepairIDs[i],
				Properties: map[string]interface{}{
					"contents": projectionContents(i, true),
					"title":    projectionTitle(i),
				},
				Vector: projectionVector(i),
			}
			require.NoError(t, common.UpdateObjectCL(t, readerURI(), obj, types.ConsistencyLevelQuorum))
		}
	})

	t.Run("RestartLaggingNode", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, 3)
		// The last id was never rewritten, so probing it cannot repair anything.
		common.WaitForNodeReadyForClass(t, laggingURI(), className,
			projectionRepairIDs[len(projectionRepairIDs)-1])
	})

	t.Run("PreconditionLaggingReplicaIsStale", func(t *testing.T) {
		for i := 0; i < rewrittenCount; i++ {
			obj, err := common.GetObjectFromNodeWithVector(t, laggingURI(), className,
				projectionRepairIDs[i], laggingNode)
			require.NoError(t, err)
			require.Equal(t, projectionContents(i, false), projectionProps(t, obj)["contents"],
				"invalid fixture: %s already holds the rewritten value for %s, so there is no divergence left to repair",
				laggingNode, projectionRepairIDs[i])
		}
	})

	t.Run("TriggerRepairWithFilteredNarrowQuery", func(t *testing.T) {
		helper.SetupClient(readerURI())
		// Where clause + selection omitting `title`/vector: the exact projection
		// that used to get written back over the replica.
		q := fmt.Sprintf(`{Get {%s(where: {path: ["contents"], operator: Like, valueText: "rewritten*"}, `+
			`consistencyLevel: ALL, limit: 100) {contents _additional {id isConsistent}}}}`, className)
		resp := common.GQLDo(t, className, q)
		require.Len(t, resp, rewrittenCount,
			"invalid fixture: the triggering query matched %d objects instead of %d, "+
				"so it did not reach the repair path for the rewritten subset", len(resp), rewrittenCount)
	})

	t.Run("RepairedReplicaKeepsUnselectedContent", func(t *testing.T) {
		for i := 0; i < rewrittenCount; i++ {
			id := projectionRepairIDs[i]

			// Confirms repair actually wrote to the lagging replica; async
			// replication is off, so nothing else could have moved this value.
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				obj, err := common.GetObjectFromNodeWithVector(t, laggingURI(), className, id, laggingNode)
				if !assert.NoError(ct, err) {
					return
				}
				assert.Equal(ct, projectionContents(i, true), projectionProps(t, obj)["contents"])
			}, 30*time.Second, 250*time.Millisecond,
				"invalid fixture: read repair never wrote %s on %s, the assertions below would be vacuous",
				id, laggingNode)

			obj, err := common.GetObjectFromNodeWithVector(t, laggingURI(), className, id, laggingNode)
			require.NoError(t, err)
			props := projectionProps(t, obj)

			// assert, not require: report property and vector loss together.
			assert.Equal(t, projectionTitle(i), props["title"],
				"read repair destroyed property %q on %s for object %s: the triggering query "+
					"did not select it, and the projected search result was written back over the full object. "+
					"properties on the repaired replica: %v", "title", laggingNode, id, props)
			assert.EqualValues(t, projectionVector(i), obj.Vector,
				"read repair destroyed the vector on %s for object %s: the triggering query "+
					"did not request it, and the projected search result was written back over the full object",
				laggingNode, id)
		}
	})

	t.Run("UntouchedObjectsAndHealthyReplicasIntact", func(t *testing.T) {
		for i := rewrittenCount; i < len(projectionRepairIDs); i++ {
			obj, err := common.GetObjectFromNodeWithVector(t, laggingURI(), className,
				projectionRepairIDs[i], laggingNode)
			require.NoError(t, err)
			assertProjectionObject(t, obj, i, false, laggingNode)
		}
		for _, node := range []struct {
			uri  string
			name string
		}{
			{compose.ContainerURI(1), docker.Weaviate0},
			{compose.ContainerURI(2), docker.Weaviate1},
		} {
			for i := 0; i < rewrittenCount; i++ {
				obj, err := common.GetObjectFromNodeWithVector(t, node.uri, className,
					projectionRepairIDs[i], node.name)
				require.NoError(t, err)
				assertProjectionObject(t, obj, i, true, node.name)
			}
		}
	})
}

func projectionProps(t assert.TestingT, obj *models.Object) map[string]interface{} {
	props, ok := obj.Properties.(map[string]interface{})
	if !assert.True(t, ok, "unexpected properties payload %T", obj.Properties) {
		return map[string]interface{}{}
	}
	return props
}

func assertProjectionObject(t assert.TestingT, obj *models.Object, i int, rewritten bool, nodeName string) {
	props := projectionProps(t, obj)
	assert.Equal(t, projectionContents(i, rewritten), props["contents"], "contents on %s", nodeName)
	assert.Equal(t, projectionTitle(i), props["title"], "title on %s", nodeName)
	assert.EqualValues(t, projectionVector(i), obj.Vector, "vector on %s", nodeName)
}
