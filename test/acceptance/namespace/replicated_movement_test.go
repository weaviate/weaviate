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

// movedObject is an object TestNamespaces_MovedShardServesReads writes. Its
// title holds token, which no other title holds.
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

func (o movedObject) model(class string) *models.Object {
	return &models.Object{
		ID: o.id, Class: class, Vector: o.vector,
		Properties: map[string]any{"title": "moved " + o.token},
	}
}

// searches returns one search at ONE per index that finds the object, each
// expected to return it alone. A limit of 10 lets an extra BM25 or filter match
// show, and the vector search asks for the nearest object only.
func (o movedObject) searches(class string) map[string]*pb.SearchRequest {
	one := pb.ConsistencyLevel_CONSISTENCY_LEVEL_ONE
	request := func(limit uint32, set func(*pb.SearchRequest)) *pb.SearchRequest {
		r := searchReq(class, limit)
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
// counts every object. Each search from searches and a REST get at ONE must
// also find the first and last object.
func requireShardServedBy(t *testing.T, node, class, key string, objects []movedObject, within time.Duration) {
	t.Helper()

	restURI, grpcURI := nodeURIs(t, node)
	rest, grpcClient := helper.NewClient(t, restURI), grpcTo(t, grpcURI)
	probes := []movedObject{objects[0], objects[len(objects)-1]}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		agg, err := grpcClient.Aggregate(authCtx(key), &pb.AggregateRequest{Collection: class, ObjectsCount: true})
		if assert.NoError(c, err, "aggregate") {
			assert.Equal(c, int64(len(objects)), agg.GetSingleResult().GetObjectsCount())
		}
		for _, o := range probes {
			for name, req := range o.searches(class) {
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
			_, err := helper.GetObjectVia(t, rest, class, o.id, types.ConsistencyLevelOne, key)
			assert.NoError(c, err, "get %s", o.token)
		}
	}, within, 500*time.Millisecond, "%q never answered every read of %s", node, class)
}

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
	helper.CreateClassAuth(t, &models.Class{
		Class:      class,
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
		VectorConfig: map[string]models.VectorConfig{
			"vec": {Vectorizer: map[string]any{"none": map[string]any{}}, VectorIndexType: "hnsw"},
		},
	}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, qualified, adminKey) })
	shardName := requireShardCountEventually(t, qualified, 1)[0]

	objects := make([]movedObject, seeded)
	batch := make([]*models.Object, seeded)
	for i := range objects {
		objects[i] = newMovedObject(i)
		batch[i] = objects[i].model(class)
	}
	helper.CreateObjectsBatchAuth(t, batch, userKey)
	// The source answers the same reads first, so a failure after the move is the move's.
	requireShardServedBy(t, sourceNode, class, userKey, objects, movementTimeout)

	opID, err := registerMovement(t, qualified, shardName, sourceNode, targetNode, api.MOVE.String())
	require.NoError(t, err)
	waitMovementState(t, opID, api.READY)
	requireSoleReplicaEventually(t, qualified, shardName, targetNode)
	// The source drops its copy, so its reads below are forwarded to the target.
	requireShardReleasedOnNode(t, qualified, shardName, sourceNode, newPresentShard(t, sourceNode))

	// A late answer after READY is a failure, so those reads get a short window.
	const servedWithin = 10 * time.Second
	requireServedByEveryNode := func(t *testing.T, within time.Duration) {
		t.Helper()
		for _, node := range []string{docker.Weaviate0, sourceNode, targetNode} {
			requireShardServedBy(t, node, class, userKey, objects, within)
		}
	}

	t.Run("every node serves every read after READY", func(t *testing.T) {
		requireServedByEveryNode(t, servedWithin)
	})

	t.Run("an object written after READY reads back from every node", func(t *testing.T) {
		added := newMovedObject(seeded)
		require.NoError(t, helper.CreateObjectAuth(t, added.model(class), userKey))
		objects = append(objects, added)
		requireServedByEveryNode(t, servedWithin)
	})

	t.Run("every node serves every read after the target restarts", func(t *testing.T) {
		withNodeStopped(t, targetNode, nil, func(*testing.T) {})
		requireServedByEveryNode(t, movementTimeout)
	})
}
