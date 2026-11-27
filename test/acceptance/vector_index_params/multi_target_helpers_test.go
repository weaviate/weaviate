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

package vector_index_params

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// Named vectors of the two-vector collections: graph is an HNSW index, ivf an
// HFresh index. Both hold the same vector per object, so the exact top k of a
// query is one ground truth for either target.
const (
	graphTarget = "graph"
	ivfTarget   = "ivf"
)

// namedIndex is one named vector of a collection created by
// createNamedVectorCollection.
type namedIndex struct {
	indexType string
	cfg       map[string]any
}

// hfreshConfig keeps postings small (8 KiB, the floor) so that a dataset of a
// few thousand 32-dim vectors spreads over hundreds of postings and the
// centroid routing budget (searchProbe) has a measurable effect on recall.
func hfreshConfig() map[string]any {
	return map[string]any{"maxPostingSizeKB": 8}
}

// twoVectorIndexes is the graph (HNSW + RQ) / ivf (HFresh) pair used by the
// per-target and multi-target tests.
func twoVectorIndexes() map[string]namedIndex {
	return map[string]namedIndex{
		graphTarget: {"hnsw", rqConfig()},
		ivfTarget:   {"hfresh", hfreshConfig()},
	}
}

// defineNamedVectorCollection creates a collection with the given named
// vectors (no vectorizer) and loads the dataset into it, every named vector
// of object i getting ds.vectors[i]. mutate, if set, adjusts the class (e.g.
// sharding or replication) before it is created. The caller owns its deletion.
func defineNamedVectorCollection(t *testing.T, name string, indexes map[string]namedIndex,
	mutate func(*models.Class), ds *dataset, n int, cl types.ConsistencyLevel,
) {
	t.Helper()
	vc := make(map[string]models.VectorConfig, len(indexes))
	for target, idx := range indexes {
		vc[target] = models.VectorConfig{
			Vectorizer:        map[string]any{"none": map[string]any{}},
			VectorIndexType:   idx.indexType,
			VectorIndexConfig: idx.cfg,
		}
	}
	class := &models.Class{Class: name, Properties: baseProperties(), VectorConfig: vc}
	if mutate != nil {
		mutate(class)
	}
	helper.CreateClass(t, class)

	got := helper.GetClass(t, name)
	for target, idx := range indexes {
		require.Equal(t, idx.indexType, got.VectorConfig[target].VectorIndexType,
			"named vector %s must be a %s index", target, idx.indexType)
	}

	insertObjects(t, name, n, cl, func(i int) *models.Object {
		vecs := make(models.Vectors, len(indexes))
		for target := range indexes {
			vecs[target] = ds.vectors[i]
		}
		return &models.Object{
			ID:      idToUUID(i),
			Class:   name,
			Vectors: vecs,
			Properties: map[string]any{
				"title":  fmt.Sprintf("object %d", i),
				"bucket": i % numBuckets,
			},
		}
	})
}

// createNamedVectorCollection is defineNamedVectorCollection for a collection
// owned by one test: it is deleted when the test ends.
func createNamedVectorCollection(t *testing.T, name string, indexes map[string]namedIndex,
	mutate func(*models.Class), ds *dataset, n int, cl types.ConsistencyLevel,
) {
	t.Helper()
	defineNamedVectorCollection(t, name, indexes, mutate, ds, n, cl)
	t.Cleanup(func() { helper.DeleteClass(t, name) })
}

// twoVectorObjects sizes the shared two-vector collection: with 8 KiB postings
// the HFresh index spreads over enough postings that routing k centroids
// (search_probe <= k) misses about a tenth of the exact neighbours, well
// beyond the 0.05 gap the tests assert (3000 objects left only 0.06).
const twoVectorObjects = 4000

func efP(ef uint32) *pb.VectorIndexParams   { return &pb.VectorIndexParams{Ef: u32(ef)} }
func probeP(p uint32) *pb.VectorIndexParams { return &pb.VectorIndexParams{SearchProbe: u32(p)} }

// namedVectorRequest is nearVectorRequest on one named vector.
func namedVectorRequest(collection, target string, vec []float32, limit uint32, params *pb.VectorIndexParams) *pb.SearchRequest {
	r := nearVectorRequest(collection, vec, limit, nil)
	r.NearVector.Targets = &pb.Targets{TargetVectors: []string{target}}
	r.VectorIndexParams = params
	return r
}

// negate returns -v, a decoy query: a zero-weight target searched with it
// adds only candidates far from q, so the combined result is the other
// target's alone (q itself would add q's true neighbours and mask a loss).
func negate(v []float32) []float32 {
	out := make([]float32, len(v))
	for i := range v {
		out[i] = -v[i]
	}
	return out
}

func singleVectors(v []float32) []*pb.Vectors {
	return []*pb.Vectors{{
		Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
		VectorBytes: byteops.Fp32SliceToBytes(v),
	}}
}

// targetQuery is one target of a multi-target near-vector request.
type targetQuery struct {
	name   string
	vec    []float32
	weight float32
}

// isolate returns the targets of a graph/ivf request where only target
// counts: it gets q with weight 1, the other target gets the decoy -q with
// weight 0 (see negate).
func isolate(target string, q []float32) []targetQuery {
	other := ivfTarget
	if target == ivfTarget {
		other = graphTarget
	}
	return []targetQuery{{target, q, 1}, {other, negate(q), 0}}
}

// both returns the graph/ivf targets searched with the same query vector.
func both(q []float32) []targetQuery {
	return []targetQuery{{graphTarget, q, 1}, {ivfTarget, q, 1}}
}

// forTarget builds one vector_index_params_for_targets entry.
func forTarget(target string, params *pb.VectorIndexParams) *pb.VectorIndexParamsForTarget {
	return &pb.VectorIndexParamsForTarget{Target: target, Params: params}
}

// multiTargetRequest builds a gRPC near-vector request over several named
// vectors, each with its own query vector. Weights are sent only for the
// MANUAL and RELATIVE_SCORE combinations, which require them.
func multiTargetRequest(collection string, limit uint32, method pb.CombinationMethod, targets []targetQuery,
	params *pb.VectorIndexParams, perTarget ...*pb.VectorIndexParamsForTarget,
) *pb.SearchRequest {
	tg := &pb.Targets{Combination: method}
	vft := make([]*pb.VectorForTarget, 0, len(targets))
	for _, tq := range targets {
		tg.TargetVectors = append(tg.TargetVectors, tq.name)
		if method == pb.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL ||
			method == pb.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE {
			tg.WeightsForTargets = append(tg.WeightsForTargets, &pb.WeightsForTarget{Target: tq.name, Weight: tq.weight})
		}
		vft = append(vft, &pb.VectorForTarget{Name: tq.name, Vectors: singleVectors(tq.vec)})
	}
	return &pb.SearchRequest{
		Collection:                  collection,
		Limit:                       limit,
		Metadata:                    &pb.MetadataRequest{Uuid: true},
		NearVector:                  &pb.NearVector{Targets: tg, VectorForTargets: vft},
		VectorIndexParams:           params,
		VectorIndexParamsForTargets: perTarget,
		Uses_127Api:                 true,
	}
}

// idsPerQuery runs build(q) for every dataset query and returns the ids.
func idsPerQuery(t *testing.T, c pb.WeaviateClient, ds *dataset, build func(q []float32) *pb.SearchRequest) [][]string {
	t.Helper()
	out := make([][]string, len(ds.queries))
	for qi, q := range ds.queries {
		out[qi] = searchIDs(t, c, build(q))
	}
	return out
}

// meanRecallOf is the mean recall@k of build(q) against the exact top k over
// every dataset query.
func meanRecallOf(t *testing.T, c pb.WeaviateClient, ds *dataset, build func(q []float32) *pb.SearchRequest) float64 {
	t.Helper()
	var sum float64
	for qi, q := range ds.queries {
		sum += recallOf(searchIDs(t, c, build(q)), ds.exactTopK(qi, everything))
	}
	return sum / float64(len(ds.queries))
}

// countDiffering returns on how many queries a and b differ.
func countDiffering(a, b [][]string) int {
	n := 0
	for i := range a {
		if !assert.ObjectsAreEqual(a[i], b[i]) {
			n++
		}
	}
	return n
}

// waitForStableResults waits until two passes return identical ids: HFresh
// rebalances postings in the background after inserts.
func waitForStableResults(t *testing.T, c pb.WeaviateClient, ds *dataset, build func(q []float32) *pb.SearchRequest) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	prev := idsPerQuery(t, c, ds, build)
	for {
		time.Sleep(time.Second)
		cur := idsPerQuery(t, c, ds, build)
		if countDiffering(prev, cur) == 0 {
			return
		}
		require.True(t, time.Now().Before(deadline), "search results did not settle")
		prev = cur
	}
}

// requireInvalidArgument asserts that err is a gRPC InvalidArgument whose
// message contains fragment.
func requireInvalidArgument(t *testing.T, err error, fragment string) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "not a gRPC status: %v", err)
	assert.Equal(t, codes.InvalidArgument, st.Code(), st.Message())
	assert.Contains(t, st.Message(), fragment)
}

// restSearch posts body to /v1/search/{class}/{searchType} and returns the
// status code and decoded response.
func restSearch(t *testing.T, uri, class, searchType string, body map[string]any) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/search/%s/%s", uri, class, searchType),
		"application/json", bytes.NewReader(payload))
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

// restResultIDs extracts the result ids of a successful REST search.
func restResultIDs(t *testing.T, out map[string]any) []string {
	t.Helper()
	res, ok := out["results"].([]any)
	require.True(t, ok, "%v", out)
	ids := make([]string, 0, len(res))
	for _, r := range res {
		ids = append(ids, r.(map[string]any)["id"].(string))
	}
	return ids
}

// restOKIDs posts a REST search that must succeed and returns its ids.
func restOKIDs(t *testing.T, uri, class, searchType string, body map[string]any) []string {
	t.Helper()
	code, out := restSearch(t, uri, class, searchType, body)
	require.Equal(t, http.StatusOK, code, "%v", out)
	return restResultIDs(t, out)
}

// setupSingleNode points the REST helper at the shared node and returns a
// gRPC client for it.
func setupSingleNode(t *testing.T) pb.WeaviateClient {
	t.Helper()
	helper.SetupClient(singleNode.GetWeaviate().URI())
	return newGRPC(t, singleNode.GetWeaviate().GrpcURI())
}

// callCtx is a short per-call context for single gRPC calls in error tables.
func callCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
