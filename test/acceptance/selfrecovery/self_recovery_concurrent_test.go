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

package selfrecovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestSelfRecoveryViaLogReplayConcurrentChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{})

	const (
		initialCount    = 500
		concurrentCount = 200
		newCollCount    = 100
		totalP          = initialCount + concurrentCount
		wipedIdx        = 2
	)
	wipedNodeName := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

	pClass := articles.ParagraphsClass()
	pClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
	pClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	pClass.Vectorizer = "none"

	qClass := articles.ParagraphsClass()
	qClass.Class = "RecoveryQ"
	qClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
	qClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	qClass.Vectorizer = "none"

	pObj := func(i int) *models.Object {
		return articles.NewParagraph().
			WithID(strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))).
			WithContents(fmt.Sprintf("p#%d", i)).Object()
	}
	qObj := func(i int) *models.Object {
		return &models.Object{
			Class:      qClass.Class,
			ID:         strfmt.UUID(fmt.Sprintf("11111111-1111-1111-1111-%012d", i)),
			Properties: map[string]interface{}{"contents": fmt.Sprintf("q#%d", i)},
		}
	}
	shardOf := func(t *testing.T, className, nodeName string) (count int64, loaded, found bool) {
		verbose := verbosity.OutputVerbose
		body, err := helper.Client(t).Nodes.NodesGetClass(
			nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), nil)
		if err != nil || body.Payload == nil {
			return 0, false, false
		}
		for _, n := range body.Payload.Nodes {
			if n.Name != nodeName {
				continue
			}
			if len(n.Shards) == 0 {
				return 0, false, true
			}
			return n.Shards[0].ObjectCount, n.Shards[0].Loaded, true
		}
		return 0, false, false
	}
	t.Run("wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
	})

	t.Run("create RF=3 collection and ingest initial data", func(t *testing.T) {
		helper.CreateClass(t, pClass)
		waitShardsLoaded(t, pClass.Class, 1)

		batch := make([]*models.Object, initialCount)
		for i := 0; i < initialCount; i++ {
			batch[i] = pObj(i + 1)
		}
		submitBatch(t, batch, types.ConsistencyLevelQuorum)

		// A peer that crash-restarted during formation may still be recovering;
		// settle and require every shard loaded so the wipe leaves enough replicas.
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		waitShardsLoaded(t, pClass.Class, 1)
	})

	t.Run("wipe node-3 and restart (rejoins via log replay)", func(t *testing.T) {
		wipeAndRestart(ctx, t, compose, wipedIdx)
	})

	t.Run("a SELF_RECOVERY op fires (wiped node shard now excluded)", func(t *testing.T) {
		waitSelfRecoveryOpFired(t, wipedNodeName)
	})

	t.Run("apply new data and schema changes during recovery", func(t *testing.T) {
		concurrent := make([]*models.Object, concurrentCount)
		for i := 0; i < concurrentCount; i++ {
			concurrent[i] = pObj(initialCount + i + 1)
		}
		submitBatch(t, concurrent, types.ConsistencyLevelQuorum)

		_, perr := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(
			clschema.NewSchemaObjectsPropertiesAddParams().
				WithClassName(pClass.Class).
				WithBody(&models.Property{Name: "category", DataType: schema.DataTypeText.PropString()}),
			nil)
		require.NoError(t, perr)

		helper.CreateClass(t, qClass)
		qBatch := make([]*models.Object, newCollCount)
		for i := 0; i < newCollCount; i++ {
			qBatch[i] = qObj(i + 1)
		}
		submitBatch(t, qBatch, types.ConsistencyLevelQuorum)
	})

	t.Run("all 3 nodes converge on P (initial+concurrent) and Q", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, name := range allNodes {
				pc, pl, pf := shardOf(t, pClass.Class, name)
				require.True(ct, pf, "P shard on %s present", name)
				assert.True(ct, pl, "P shard on %s loaded", name)
				assert.Equal(ct, int64(totalP), pc, "P object count on %s", name)

				qc, ql, qf := shardOf(t, qClass.Class, name)
				require.True(ct, qf, "Q shard on %s present", name)
				assert.True(ct, ql, "Q shard on %s loaded", name)
				assert.Equal(ct, int64(newCollCount), qc, "Q object count on %s", name)
			}
		}, 8*time.Minute, 2*time.Second)
	})

	t.Run("schema change reached the wiped node", func(t *testing.T) {
		helper.SetupClient(compose.ContainerURI(wipedIdx))
		defer helper.SetupClient(compose.GetWeaviate().URI())
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			cls := helper.GetClass(t, pClass.Class)
			require.NotNil(ct, cls)
			names := make([]string, 0, len(cls.Properties))
			for _, p := range cls.Properties {
				names = append(names, p.Name)
			}
			assert.Contains(ct, names, "category", "wiped node P schema has the new property")
		}, 1*time.Minute, 1*time.Second)
	})

	t.Run("direct reads at consistency=ONE on the wiped node return recovered data", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for _, i := range []int{1, initialCount, totalP} {
				id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
				exists, err := common.ObjectExistsCL(t, compose.ContainerURI(wipedIdx), pClass.Class, id, types.ConsistencyLevelOne)
				assert.NoError(ct, err)
				assert.True(ct, exists, "P object %s missing on wiped node", id)
			}
			qid := strfmt.UUID(fmt.Sprintf("11111111-1111-1111-1111-%012d", newCollCount))
			exists, err := common.ObjectExistsCL(t, compose.ContainerURI(wipedIdx), qClass.Class, qid, types.ConsistencyLevelOne)
			assert.NoError(ct, err)
			assert.True(ct, exists, "Q object %s missing on wiped node", qid)
		}, 2*time.Minute, 2*time.Second)
	})
}
