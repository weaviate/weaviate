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

package namespace

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// movedObject is an object the movement tests write. Its title holds token,
// which no other title in its shard holds.
type movedObject struct {
	id     strfmt.UUID
	token  string
	vector []float32
}

func newMovedObject(i int) movedObject {
	vector := make([]float32, 8)
	for d := range vector {
		vector[d] = rand.Float32()*2 - 1
	}
	return movedObject{id: strfmt.UUID(uuid.NewString()), token: fmt.Sprintf("item%d", i), vector: vector}
}

// model returns the object for tenant of class, where tenant is empty for a
// class without tenants.
func (o movedObject) model(class, tenant string) *models.Object {
	return &models.Object{
		ID: o.id, Class: class, Tenant: tenant, Vector: o.vector,
		Properties: map[string]any{"title": "moved " + o.token},
	}
}

// searchableClass returns a class with the title property and the vector that
// the searches of movedObject read.
func searchableClass(name string) *models.Class {
	return &models.Class{
		Class:      name,
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
		VectorConfig: map[string]models.VectorConfig{
			"vec": {Vectorizer: map[string]any{"none": map[string]any{}}, VectorIndexType: "hnsw"},
		},
	}
}

// seedMovedObjects writes n new objects into tenant of class and returns them.
func seedMovedObjects(t *testing.T, class, tenant, key string, n int) []movedObject {
	t.Helper()

	objects := make([]movedObject, n)
	batch := make([]*models.Object, n)
	for i := range objects {
		objects[i] = newMovedObject(i)
		batch[i] = objects[i].model(class, tenant)
	}
	helper.CreateObjectsBatchAuth(t, batch, key)
	require.False(t, t.Failed(), "the seed batch refused an object")
	return objects
}

// searches returns one search at ONE per index that finds the object, each
// expected to return it alone. A limit of 10 lets an extra BM25 or filter match
// show, and the vector search asks for the nearest object only.
func (o movedObject) searches(class, tenant string) map[string]*pb.SearchRequest {
	one := pb.ConsistencyLevel_CONSISTENCY_LEVEL_ONE
	request := func(limit uint32, set func(*pb.SearchRequest)) *pb.SearchRequest {
		r := searchReq(class, limit)
		r.Tenant = tenant
		r.ConsistencyLevel = &one
		r.Metadata = &pb.MetadataRequest{Uuid: true}
		set(r)
		return r
	}
	return map[string]*pb.SearchRequest{
		"near vector": request(1, func(r *pb.SearchRequest) {
			r.NearVector = &pb.NearVector{Vectors: []*pb.Vectors{{
				VectorBytes: byteops.Fp32SliceToBytes(o.vector), Type: pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
			}}}
		}),
		"bm25": request(10, func(r *pb.SearchRequest) {
			r.Bm25Search = &pb.BM25{Query: o.token, Properties: []string{"title"}}
		}),
		"filter": request(10, func(r *pb.SearchRequest) {
			r.Filters = &pb.Filters{
				Operator:  pb.Filters_OPERATOR_EQUAL,
				TestValue: &pb.Filters_ValueText{ValueText: o.token},
				Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "title"}},
			}
		}),
	}
}

// requireShardServedBy polls node for up to within until its gRPC aggregate
// counts every object of tenant. Each search from searches and a REST get at
// ONE must also find the first and last object.
func requireShardServedBy(t *testing.T, node, class, tenant, key string, objects []movedObject, within time.Duration) {
	t.Helper()

	restURI, grpcURI := nodeURIs(t, node)
	rest, grpcClient := helper.NewClient(t, restURI), grpcTo(t, grpcURI)
	probes := []movedObject{objects[0], objects[len(objects)-1]}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		agg, err := grpcClient.Aggregate(authCtx(key), &pb.AggregateRequest{Collection: class, Tenant: tenant, ObjectsCount: true})
		if assert.NoError(c, err, "aggregate") {
			assert.Equal(c, int64(len(objects)), agg.GetSingleResult().GetObjectsCount())
		}
		for _, o := range probes {
			for name, req := range o.searches(class, tenant) {
				resp, err := grpcClient.Search(authCtx(key), req)
				if !assert.NoError(c, err, "%s for %s", name, o.token) {
					continue
				}
				ids := make([]string, 0, len(resp.Results))
				for _, r := range resp.Results {
					ids = append(ids, r.GetMetadata().GetId())
				}
				assert.Equal(c, []string{o.id.String()}, ids, "%s for %s", name, o.token)
			}
			_, err := helper.GetObjectVia(t, rest, class, tenant, o.id, types.ConsistencyLevelOne, key)
			assert.NoError(c, err, "get %s", o.token)
		}
	}, within, 500*time.Millisecond, "%q never answered every read of %s %q", node, class, tenant)
}

// servedAfterReadyWithin bounds the reads after a movement reaches READY, since
// a late answer then is a failure.
const servedAfterReadyWithin = 10 * time.Second

// TestNamespaces_MovedShardServesReads reads a moved namespaced shard through
// every node once a MOVE has left the target its only replica. Not parallel,
// because it restarts the target.
func TestNamespaces_MovedShardServesReads(t *testing.T) {
	const (
		sourceNode = docker.Weaviate1
		targetNode = restartNodeName
		class      = "MovedShard"
		seeded     = 100
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	qualified := ns + ":" + class
	helper.CreateClassAuth(t, searchableClass(class), userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, qualified, adminKey) })
	shardName := requireShardCountEventually(t, qualified, 1)[0]

	objects := seedMovedObjects(t, class, "", userKey, seeded)
	// The source answers the same reads first, so a failure after the move is the move's.
	requireShardServedBy(t, sourceNode, class, "", userKey, objects, movementTimeout)

	opID, err := registerMovement(t, qualified, shardName, sourceNode, targetNode, api.MOVE.String())
	require.NoError(t, err)
	waitMovementState(t, opID, api.READY)
	requireReplicasEventually(t, qualified, shardName, targetNode)
	// The source drops its copy, so its reads below are forwarded to the target.
	requireShardReleasedOnNode(t, qualified, shardName, sourceNode, newPresentShard(t, sourceNode))

	requireServedByEveryNode := func(t *testing.T, within time.Duration) {
		t.Helper()
		for _, node := range []string{docker.Weaviate0, sourceNode, targetNode} {
			requireShardServedBy(t, node, class, "", userKey, objects, within)
		}
	}

	t.Run("every node serves every read after READY", func(t *testing.T) {
		requireServedByEveryNode(t, servedAfterReadyWithin)
	})

	t.Run("an object written after READY reads back from every node", func(t *testing.T) {
		added := newMovedObject(seeded)
		require.NoError(t, helper.CreateObjectAuth(t, added.model(class, ""), userKey))
		objects = append(objects, added)
		requireServedByEveryNode(t, servedAfterReadyWithin)
	})

	t.Run("every node serves every read after the target restarts", func(t *testing.T) {
		withNodeStopped(t, targetNode, nil, func(*testing.T) {})
		requireServedByEveryNode(t, movementTimeout)
	})
}

// requireReplicaHoldsEventually waits until node lists the shard with want
// objects. A read through node is answered by another replica while node holds
// no copy, and ObjectCount counts only flushed segments, which a freeze writes.
func requireReplicaHoldsEventually(t *testing.T, qualifiedClass, shardName, node string, want int) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		shard, _, err := shardOnNode(t, qualifiedClass, shardName, node)
		if !assert.NoError(c, err) || !assert.NotNil(c, shard, "%q lists no shard %q", node, shardName) {
			return
		}
		assert.Equal(c, int64(want), shard.ObjectCount)
	}, movementTimeout, 500*time.Millisecond, "%q never reported %d objects in shard %q", node, want, shardName)
}

// seededShard is a shard of TestNamespaces_MoveAfterHomeNodesChange and the
// objects written to it.
type seededShard struct {
	classShard
	label   string
	objects []movedObject
}

// TestNamespaces_MoveAfterHomeNodesChange swaps weaviate-2 for weaviate-0 in the
// home_nodes of a namespace holding an RF=2 class and two RF=2 tenants. The
// tenant no MOVE touches shows that an unfreeze alone follows the new home_nodes.
func TestNamespaces_MoveAfterHomeNodesChange(t *testing.T) {
	t.Parallel()

	const (
		kept, dropped, added  = docker.Weaviate1, docker.Weaviate2, docker.Weaviate0
		docsClass, booksClass = "Docs", "Books"
		seeded                = 20
	)
	oldHome, newHome := []string{kept, dropped}, []string{kept, added}

	ns := uniqueNS()
	helper.CreateNamespaceWithHomeNodes(t, ns, oldHome, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })
	// dropped keeps listing this shard while the moved replicas leave it.
	live := newPresentShard(t, dropped)

	books := searchableClass(booksClass)
	books.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	for _, class := range []*models.Class{searchableClass(docsClass), books} {
		class.ReplicationConfig = &models.ReplicationConfig{Factor: 2}
		helper.CreateClassAuth(t, class, userKey)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+class.Class, adminKey) })
	}
	helper.CreateTenantsAuth(t, booksClass, []*models.Tenant{{Name: "moved"}, {Name: "stale"}}, userKey)

	docsShard := requireShardCountEventually(t, ns+":"+docsClass, 1)[0]
	shards := []seededShard{
		{label: "the class's shard", classShard: classShard{class: docsClass, shard: docsShard}},
		{label: "a tenant", classShard: classShard{class: booksClass, tenant: "moved", shard: "moved"}},
		{label: "a tenant no MOVE touches", classShard: classShard{class: booksClass, tenant: "stale", shard: "stale"}},
	}
	for i := range shards {
		shards[i].objects = seedMovedObjects(t, shards[i].class, shards[i].tenant, userKey, seeded)
	}

	helper.UpdateNamespace(t, ns, newHome, adminKey)
	requireEveryNodeCaughtUp(t)

	t.Run("the existing replicas stay on the old home nodes", func(t *testing.T) {
		for _, s := range shards {
			replicas, err := shardReplicas(t, ns+":"+s.class, s.shard)
			require.NoError(t, err)
			assert.ElementsMatch(t, oldHome, replicas, s.label)
		}
	})

	t.Run("a new class and a new tenant land on the new home nodes", func(t *testing.T) {
		const freshClass, freshTenant = "Fresh", "fresh"
		helper.CreateClassAuth(t, &models.Class{
			Class: freshClass, ReplicationConfig: &models.ReplicationConfig{Factor: 2},
		}, userKey)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+freshClass, adminKey) })
		helper.CreateTenantsAuth(t, booksClass, []*models.Tenant{{Name: freshTenant}}, userKey)

		freshShard := requireShardCountEventually(t, ns+":"+freshClass, 1)[0]
		requireReplicasEventually(t, ns+":"+freshClass, freshShard, newHome...)
		requireReplicasEventually(t, ns+":"+booksClass, freshTenant, newHome...)
	})

	for _, s := range shards[:2] {
		t.Run(fmt.Sprintf("a MOVE from %s to %s leaves %s served there", dropped, added, s.label), func(t *testing.T) {
			qualified := ns + ":" + s.class
			opID, err := registerMovement(t, qualified, s.shard, dropped, added, api.MOVE.String())
			require.NoError(t, err)
			waitMovementState(t, opID, api.READY)
			requireReplicasEventually(t, qualified, s.shard, newHome...)
			requireShardReleasedOnNode(t, qualified, s.shard, dropped, live)
			requireShardServedBy(t, added, s.class, s.tenant, userKey, s.objects, servedAfterReadyWithin)
		})
	}

	t.Run("an unfreeze puts both tenants on the new home nodes", func(t *testing.T) {
		tenants := shards[1:]
		setStatus := func(status string) {
			update := make([]*models.Tenant, 0, len(tenants))
			for _, s := range tenants {
				update = append(update, &models.Tenant{Name: s.tenant, ActivityStatus: status})
			}
			helper.UpdateTenantsWithAuthz(t, booksClass, update, helper.CreateAuth(userKey))
			for _, s := range tenants {
				waitForTenantStatus(t, ns+":"+booksClass, s.tenant, status, adminKey)
			}
		}
		setStatus(models.TenantActivityStatusFROZEN)
		setStatus(models.TenantActivityStatusHOT)

		for _, s := range tenants {
			requireReplicasEventually(t, ns+":"+booksClass, s.shard, newHome...)
			// Each node downloads its replica after the status reads HOT.
			for _, node := range newHome {
				requireShardServedBy(t, node, booksClass, s.tenant, userKey, s.objects, movementTimeout)
				requireReplicaHoldsEventually(t, ns+":"+booksClass, s.shard, node, len(s.objects))
			}
		}
	})
}
