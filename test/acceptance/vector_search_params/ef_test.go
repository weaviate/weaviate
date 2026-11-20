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

package vector_search_params

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	dims       = 32
	k          = 10
	numObjects = 3000
	numQueries = 30
	// statQueries is the query count for the statistical multi-node checks.
	statQueries = 100
	numBuckets  = 10
	batchSize   = 500
)

// dataset holds deterministic random vectors; the object at index i has the
// UUID returned by idFor(i) and bucket i % numBuckets.
type dataset struct {
	vectors [][]float32
	queries [][]float32
}

func newDataset(seed int64, n, queries int) *dataset {
	rng := rand.New(rand.NewSource(seed))
	gen := func() []float32 {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		return v
	}
	ds := &dataset{}
	for i := 0; i < n; i++ {
		ds.vectors = append(ds.vectors, gen())
	}
	for i := 0; i < queries; i++ {
		ds.queries = append(ds.queries, gen())
	}
	return ds
}

func idFor(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", i) }

func cosineDistance(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
}

// exactTopK returns the ids of the k exact nearest neighbours of q by cosine
// distance among the vectors accepted by keep (nil keeps all).
func (d *dataset) exactTopK(q []float32, keep func(i int) bool) []string {
	return d.exactTopN(q, k, keep)
}

// exactTopN is exactTopK for an arbitrary result size n.
func (d *dataset) exactTopN(q []float32, n int, keep func(i int) bool) []string {
	type scored struct {
		i    int
		dist float64
	}
	var all []scored
	for i, v := range d.vectors {
		if keep != nil && !keep(i) {
			continue
		}
		all = append(all, scored{i, cosineDistance(q, v)})
	}
	sort.Slice(all, func(a, b int) bool { return all[a].dist < all[b].dist })
	ids := make([]string, 0, n)
	for _, s := range all[:n] {
		ids = append(ids, idFor(s.i))
	}
	return ids
}

func recallOf(got, want []string) float64 {
	set := map[string]struct{}{}
	for _, id := range want {
		set[id] = struct{}{}
	}
	hit := 0
	for _, id := range got {
		if _, ok := set[id]; ok {
			hit++
		}
	}
	return float64(hit) / float64(len(want))
}

func insertObjects(t *testing.T, class string, n int, cl types.ConsistencyLevel, build func(i int) *models.Object) {
	t.Helper()
	for start := 0; start < n; start += batchSize {
		end := min(start+batchSize, n)
		objs := make([]*models.Object, 0, end-start)
		for i := start; i < end; i++ {
			objs = append(objs, build(i))
		}
		helper.CreateObjectsBatchCL(t, objs, cl)
	}
}

func baseProperties() []*models.Property {
	return []*models.Property{
		{Name: "title", DataType: schema.DataTypeText.PropString()},
		{Name: "bucket", DataType: schema.DataTypeInt.PropString()},
	}
}

// createSingleVectorCollection creates a legacy single-vector collection and
// loads the dataset into it.
func createSingleVectorCollection(t *testing.T, name, indexType string, indexCfg map[string]any,
	repl *models.ReplicationConfig, ds *dataset, n int, cl types.ConsistencyLevel,
) {
	t.Helper()
	class := &models.Class{
		Class:             name,
		Vectorizer:        "none",
		VectorIndexType:   indexType,
		VectorIndexConfig: indexCfg,
		Properties:        baseProperties(),
		ReplicationConfig: repl,
	}
	helper.CreateClass(t, class)
	t.Cleanup(func() { helper.DeleteClass(t, name) })
	insertObjects(t, name, n, cl, func(i int) *models.Object {
		return &models.Object{
			ID:     idToUUID(i),
			Class:  name,
			Vector: ds.vectors[i],
			Properties: map[string]any{
				"title":  fmt.Sprintf("object %d", i),
				"bucket": i % numBuckets,
			},
		}
	})
}

func rqConfig() map[string]any {
	return map[string]any{
		"ef":             -1,
		"efConstruction": 128,
		"rq":             map[string]any{"enabled": true, "bits": 8},
	}
}

func newGRPC(t *testing.T, host string) pb.WeaviateClient {
	t.Helper()
	conn, err := helper.CreateGrpcConnectionClient(host)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return helper.CreateGrpcWeaviateClient(conn)
}

func efParams(ef *uint32) *pb.VectorSearchParams {
	if ef == nil {
		return nil
	}
	return &pb.VectorSearchParams{Ef: ef}
}

func u32(v uint32) *uint32 { return &v }

func idsOf(t *testing.T, reply *pb.SearchReply, err error) []string {
	t.Helper()
	require.NoError(t, err)
	ids := make([]string, 0, len(reply.Results))
	for _, r := range reply.Results {
		ids = append(ids, r.Metadata.Id)
	}
	return ids
}

func nearVectorRequest(collection string, vec []float32, limit uint32, ef *uint32) *pb.SearchRequest {
	return &pb.SearchRequest{
		Collection: collection,
		Limit:      limit,
		Metadata:   &pb.MetadataRequest{Uuid: true},
		NearVector: &pb.NearVector{
			Vectors: []*pb.Vectors{{
				Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				VectorBytes: byteops.Fp32SliceToBytes(vec),
			}},
		},
		VectorSearchParams: efParams(ef),
		Uses_127Api:        true,
	}
}

func searchIDs(t *testing.T, c pb.WeaviateClient, req *pb.SearchRequest) []string {
	t.Helper()
	reply, err := c.Search(context.Background(), req)
	return idsOf(t, reply, err)
}

func meanRecall(t *testing.T, c pb.WeaviateClient, ds *dataset, collection string, ef *uint32,
	filter *pb.Filters, keep func(i int) bool,
) float64 {
	t.Helper()
	var sum float64
	for _, q := range ds.queries {
		req := nearVectorRequest(collection, q, k, ef)
		req.Filters = filter
		sum += recallOf(searchIDs(t, c, req), ds.exactTopK(q, keep))
	}
	return sum / float64(len(ds.queries))
}

func TestVectorSearchParamsEF_HNSW(t *testing.T) {
	helper.SetupClient(singleNode.GetWeaviate().URI())
	grpcClient := newGRPC(t, singleNode.GetWeaviate().GrpcURI())
	ds := newDataset(42, numObjects, statQueries)

	t.Run("unset equals zero", func(t *testing.T) {
		createSingleVectorCollection(t, "EfUnsetZero", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)
		for qi, q := range ds.queries {
			unset := searchIDs(t, grpcClient, nearVectorRequest("EfUnsetZero", q, k, nil))
			zero := searchIDs(t, grpcClient, nearVectorRequest("EfUnsetZero", q, k, u32(0)))
			require.Equal(t, unset, zero, "query %d", qi)
		}
	})

	t.Run("recall non-decreasing", func(t *testing.T) {
		createSingleVectorCollection(t, "EfRecall", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)
		prev := -1.0
		recalls := map[uint32]float64{}
		for _, ef := range []uint32{10, 50, 200, 1000} {
			r := meanRecall(t, grpcClient, ds, "EfRecall", u32(ef), nil, nil)
			t.Logf("ef=%d recall@%d=%.3f", ef, k, r)
			// 0.01 tolerance: RQ rescoring can invert two recalls both near 1.0.
			assert.GreaterOrEqual(t, r, prev-0.01, "recall must not decrease when ef grows to %d", ef)
			prev = r
			recalls[ef] = r
		}
		assert.GreaterOrEqual(t, prev, 0.95, "recall at ef=1000")
		// The collection default ef already reaches ~1.0 recall, so only a low
		// ef distinguishes "ef reached HNSW" from "ef was silently dropped".
		assert.Less(t, recalls[10], recalls[1000], "ef=10 must be strictly worse than ef=1000")
		assert.Less(t, recalls[10], 0.95, "ef=10 recall")
	})

	t.Run("ef below limit", func(t *testing.T) {
		createSingleVectorCollection(t, "EfBelowLimit", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)
		// The beam is floored at the limit, so ef=10 and ef=100 both search
		// with a beam of 100 and must return the same 100 ids.
		for qi, q := range ds.queries {
			floored := searchIDs(t, grpcClient, nearVectorRequest("EfBelowLimit", q, 100, u32(10)))
			require.Len(t, floored, 100, "query %d", qi)
			atLimit := searchIDs(t, grpcClient, nearVectorRequest("EfBelowLimit", q, 100, u32(100)))
			require.Equal(t, atLimit, floored, "query %d: ef below limit must behave as ef=limit", qi)
		}
	})

	t.Run("with filter", func(t *testing.T) {
		// bucketBelow keeps the objects whose bucket is < n, i.e. n*10% of them.
		bucketBelow := func(n int) (*pb.Filters, func(i int) bool) {
			return &pb.Filters{
				Operator:  pb.Filters_OPERATOR_LESS_THAN,
				TestValue: &pb.Filters_ValueInt{ValueInt: int64(n)},
				Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "bucket"}},
			}, func(i int) bool { return i%numBuckets < n }
		}
		// With the default flatSearchCutoff (40000) every allow list here is
		// brute-forced and ef only caps the compressed candidates kept for
		// rescoring, so the gap is tiny; with flatSearchCutoff 0 the graph is
		// walked and the filter strategy sees the override, which must then
		// open a clear recall gap. The graph cases keep 50% of the objects: on
		// a 10% filter sweeping explores so much of the graph to find allowed
		// candidates that ef=10 is already near-exact (measured gap 0.01), while
		// at 50% all three strategies lose about 0.10 recall at ef=10.
		cases := []struct {
			name      string
			cutoff    int
			strategy  string
			buckets   int // selectivity: buckets of 10 kept
			strictGap bool
		}{
			{"brute force below flatSearchCutoff", -1, "", 1, false},
			{"graph sweeping", 0, "sweeping", 5, true},
			{"graph acorn", 0, "acorn", 5, true},
			{"graph pathseer", 0, "pathseer", 5, true},
		}
		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cfg := rqConfig()
				if tc.cutoff >= 0 {
					cfg["flatSearchCutoff"] = tc.cutoff
				}
				if tc.strategy != "" {
					cfg["filterStrategy"] = tc.strategy
				}
				filter, keep := bucketBelow(tc.buckets)
				class := fmt.Sprintf("EfFilter%d", i)
				createSingleVectorCollection(t, class, "hnsw", cfg, nil, ds, numObjects, types.ConsistencyLevelQuorum)
				low := meanRecall(t, grpcClient, ds, class, u32(10), filter, keep)
				high := meanRecall(t, grpcClient, ds, class, u32(1000), filter, keep)
				t.Logf("%s: filtered recall ef=10 %.3f, ef=1000 %.3f", tc.name, low, high)
				assert.GreaterOrEqual(t, high, 0.95, "ef=1000 must be near-exact")
				if tc.strictGap {
					assert.LessOrEqual(t, low, high-0.03, "ef=10 must have clearly lower recall on the graph path")
				} else {
					assert.GreaterOrEqual(t, high, low)
				}
			})
		}
	})

	for _, muvera := range []bool{false, true} {
		name := "multi-vector"
		if muvera {
			name = "multi-vector muvera"
		}
		t.Run(name, func(t *testing.T) {
			testMultiVector(t, grpcClient, muvera)
		})
	}

	t.Run("hybrid dense leg", func(t *testing.T) {
		// The hybrid dense leg fetches max(limit, QUERY_HYBRID_MAXIMUM_RESULTS=100)
		// candidates, so its beam is floored at 100 and an ef below 100 is a
		// no-op. Recall cannot tell beam 100 from beam 1000 at k=100 (both are
		// ~1.0), so the check is exact equality with the equivalent near-vector
		// request, made informative by requiring that the two references (beam
		// 100 and beam 1000) differ on some queries. The collection gets ef
		// 1000 so that the default beam is the wide one.
		const hybridLimit = 100
		cfg := rqConfig()
		cfg["ef"] = 1000
		createSingleVectorCollection(t, "EfHybrid", "hnsw", cfg, nil, ds, numObjects, types.ConsistencyLevelQuorum)
		alpha := float32(1)
		hybridIDs := func(q []float32, ef *uint32) []string {
			ids := searchIDs(t, grpcClient, &pb.SearchRequest{
				Collection: "EfHybrid",
				Limit:      hybridLimit,
				Metadata:   &pb.MetadataRequest{Uuid: true},
				HybridSearch: &pb.Hybrid{
					Query:         "object",
					AlphaParam:    &alpha,
					UseAlphaParam: true,
					Vectors: []*pb.Vectors{{
						Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
						VectorBytes: byteops.Fp32SliceToBytes(q),
					}},
				},
				VectorSearchParams: efParams(ef),
				Uses_127Api:        true,
			})
			require.Len(t, ids, hybridLimit)
			return ids
		}
		differs := 0
		for qi, q := range ds.queries {
			narrow := searchIDs(t, grpcClient, nearVectorRequest("EfHybrid", q, hybridLimit, u32(100)))
			wide := searchIDs(t, grpcClient, nearVectorRequest("EfHybrid", q, hybridLimit, u32(1000)))
			if !assert.ObjectsAreEqual(narrow, wide) {
				differs++
			}
			// alpha 1: the hybrid result is the dense leg, so it must equal the
			// near-vector request with the same limit and ef...
			require.Equal(t, narrow, hybridIDs(q, u32(100)), "request ef 100, query %d", qi)
			require.Equal(t, wide, hybridIDs(q, u32(1000)), "request ef 1000, query %d", qi)
			// ...and without params it must use the collection default (1000).
			require.Equal(t, wide, hybridIDs(q, nil), "collection ef, query %d", qi)
		}
		t.Logf("hybrid: beam 100 and beam 1000 differ on %d of %d queries", differs, len(ds.queries))
		require.Positive(t, differs, "beam 100 and 1000 never differ, so the equalities above prove nothing")
	})

	t.Run("errors", func(t *testing.T) {
		createSingleVectorCollection(t, "EfErrors", "hnsw", rqConfig(), nil, ds, 100, types.ConsistencyLevelQuorum)
		_, err := grpcClient.Search(context.Background(), nearVectorRequest("EfErrors", ds.queries[0], k, u32(10001)))
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		assert.Contains(t, st.Message(), "HNSW_MAX_SEARCH_EF")
	})
}

func idToUUID(i int) strfmt.UUID { return strfmt.UUID(idFor(i)) }

// testMultiVector checks that a per-request ef is accepted and returns k
// results on a multi-vector (ColBERT-style) named vector, with or without
// MUVERA.
func testMultiVector(t *testing.T, c pb.WeaviateClient, muvera bool) {
	const (
		tokens = 3
		n      = 1000
	)
	rng := rand.New(rand.NewSource(7))
	token := func() []float32 {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		return v
	}
	multi := func() [][]float32 {
		out := make([][]float32, tokens)
		for i := range out {
			out[i] = token()
		}
		return out
	}

	name := "EfMultiVector"
	multivector := map[string]any{"enabled": true, "aggregation": "maxSim"}
	if muvera {
		name = "EfMultiVectorMuvera"
		multivector["muvera"] = map[string]any{
			"enabled": true, "ksim": 4, "dprojections": 16, "repetitions": 10,
		}
	}
	class := &models.Class{
		Class:      name,
		Properties: baseProperties(),
		VectorConfig: map[string]models.VectorConfig{
			"colbert": {
				Vectorizer:        map[string]any{"none": map[string]any{}},
				VectorIndexType:   "hnsw",
				VectorIndexConfig: map[string]any{"multivector": multivector},
			},
		},
	}
	helper.CreateClass(t, class)
	t.Cleanup(func() { helper.DeleteClass(t, name) })
	insertObjects(t, name, n, types.ConsistencyLevelQuorum, func(i int) *models.Object {
		return &models.Object{
			ID:         idToUUID(i),
			Class:      name,
			Vectors:    models.Vectors{"colbert": multi()},
			Properties: map[string]any{"title": fmt.Sprintf("object %d", i), "bucket": i % numBuckets},
		}
	})

	search := func(q [][]float32, ef uint32) []string {
		reply, err := c.Search(context.Background(), &pb.SearchRequest{
			Collection: name,
			Limit:      k,
			Metadata:   &pb.MetadataRequest{Uuid: true},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					Type:        pb.Vectors_VECTOR_TYPE_MULTI_FP32,
					VectorBytes: byteops.Fp32SliceOfSlicesToBytes(q),
				}},
				Targets: &pb.Targets{TargetVectors: []string{"colbert"}},
			},
			VectorSearchParams: efParams(u32(ef)),
			Uses_127Api:        true,
		})
		ids := idsOf(t, reply, err)
		require.Len(t, ids, k)
		return ids
	}

	// ef=1000 visits every document (1000 candidates, or all of them after
	// the MUVERA pre-search), so late interaction over that set is the exact
	// MaxSim top k and serves as the reference. ef=10 pools only a few dozen
	// candidates and must lose recall against it, otherwise the override
	// never reached the multi-vector path.
	var sum float64
	for qi := 0; qi < numQueries; qi++ {
		q := multi()
		sum += recallOf(search(q, 10), search(q, 1000))
	}
	mean := sum / numQueries
	t.Logf("muvera=%v: mean recall@%d of ef=10 against ef=1000: %.3f", muvera, k, mean)
	assert.LessOrEqual(t, mean, 0.9, "ef=10 must return a different top k than ef=1000")
}

func TestVectorSearchParamsEF_REST(t *testing.T) {
	uri := singleNode.GetWeaviate().URI()
	helper.SetupClient(uri)
	grpcClient := newGRPC(t, singleNode.GetWeaviate().GrpcURI())
	ds := newDataset(43, numObjects, numQueries)
	createSingleVectorCollection(t, "EfREST", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)

	post := func(searchType string, body map[string]any) (int, map[string]any) {
		t.Helper()
		payload, err := json.Marshal(body)
		require.NoError(t, err)
		resp, err := http.Post(fmt.Sprintf("http://%s/v1/search/EfREST/%s", uri, searchType),
			"application/json", bytes.NewReader(payload))
		require.NoError(t, err)
		defer resp.Body.Close()
		var out map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		return resp.StatusCode, out
	}

	restIDs := func(out map[string]any) []string {
		t.Helper()
		res, ok := out["results"].([]any)
		require.True(t, ok, "%v", out)
		ids := make([]string, 0, len(res))
		for _, r := range res {
			ids = append(ids, r.(map[string]any)["id"].(string))
		}
		return ids
	}

	tests := []struct {
		name       string
		searchType string
		body       map[string]any
		wantStatus int
	}{
		{"near-vector", "near-vector", map[string]any{"vector": ds.queries[0], "vectorSearchParams": map[string]any{"ef": 800}}, http.StatusOK},
		{"near-object", "near-object", map[string]any{"id": idFor(0), "vectorSearchParams": map[string]any{"ef": 800}}, http.StatusOK},
		{"hybrid", "hybrid", map[string]any{"query": "object", "alpha": 1, "vector": ds.queries[0], "vectorSearchParams": map[string]any{"ef": 800}}, http.StatusOK},
		{"negative ef", "near-vector", map[string]any{"vector": ds.queries[0], "vectorSearchParams": map[string]any{"ef": -1}}, http.StatusUnprocessableEntity},
		{"ef above cap", "near-vector", map[string]any{"vector": ds.queries[0], "vectorSearchParams": map[string]any{"ef": 10001}}, http.StatusUnprocessableEntity},
		{"bm25 rejects params", "bm25", map[string]any{"query": "object", "vectorSearchParams": map[string]any{"ef": 10}}, http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.body["limit"] = k
			status, out := post(tt.searchType, tt.body)
			require.Equal(t, tt.wantStatus, status, "%v", out)
			if tt.wantStatus == http.StatusOK {
				assert.Len(t, restIDs(out), k)
			}
		})
	}

	// REST must land on the same search as gRPC: for every query, the ids at
	// ef=10 and at ef=1000 are identical across the two surfaces. The gRPC
	// path is proven ef-sensitive above, so this pins the REST mapping without
	// a second statistical check.
	t.Run("matches gRPC per query", func(t *testing.T) {
		for _, ef := range []uint32{10, 1000} {
			for qi, q := range ds.queries {
				status, out := post("near-vector", map[string]any{
					"vector": q, "limit": k, "vectorSearchParams": map[string]any{"ef": ef},
				})
				require.Equal(t, http.StatusOK, status, "%v", out)
				want := searchIDs(t, grpcClient, nearVectorRequest("EfREST", q, k, u32(ef)))
				require.Equal(t, want, restIDs(out), "ef %d query %d", ef, qi)
			}
		}
	})
}

func TestVectorSearchParamsEF_FlatIgnores(t *testing.T) {
	helper.SetupClient(singleNode.GetWeaviate().URI())
	grpcClient := newGRPC(t, singleNode.GetWeaviate().GrpcURI())
	ds := newDataset(44, 1000, numQueries)
	createSingleVectorCollection(t, "EfFlat", "flat", nil, nil, ds, 1000, types.ConsistencyLevelQuorum)

	for qi, q := range ds.queries {
		with := searchIDs(t, grpcClient, nearVectorRequest("EfFlat", q, k, u32(1000)))
		without := searchIDs(t, grpcClient, nearVectorRequest("EfFlat", q, k, nil))
		require.Len(t, with, k, "query %d", qi)
		require.Len(t, without, k, "query %d", qi)
		require.Equal(t, without, with, "query %d", qi)
	}
}

// assertEFSensitivity checks, per node, the mean recall@10 over the dataset
// queries: ef=1000 must be near-exact (>= 0.95) and ef=10 must be at least 0.03
// lower. A mean over 1000 ranked items is a more stable statistic than a count
// of near-perfect queries. With 100 queries the standard error of the mean
// recall@10 is roughly 0.01, so a 0.03 gap is about three standard errors
// (0.05 on 30 queries was under one, i.e. a coin flip).
func assertEFSensitivity(t *testing.T, compose *docker.DockerCompose, ds *dataset, class string) {
	for node := 1; node <= 3; node++ {
		t.Run(fmt.Sprintf("node %d", node), func(t *testing.T) {
			c := newGRPC(t, compose.GetWeaviateNode(node).GrpcURI())
			high := meanRecall(t, c, ds, class, u32(1000), nil, nil)
			low := meanRecall(t, c, ds, class, u32(10), nil, nil)
			t.Logf("node %d: mean recall@%d ef=1000 %.3f, ef=10 %.3f", node, k, high, low)
			assert.GreaterOrEqual(t, high, 0.95)
			assert.LessOrEqual(t, low, high-0.03, "ef=10 must have clearly lower mean recall than ef=1000")
		})
	}
}

// TestVectorSearchParamsEF_Cluster shares one 3-node cluster between the
// replicated (RF=3) and sharded (RF=1, 3 shards) scenarios.
func TestVectorSearchParamsEF_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithWeaviateClusterWithGRPC().Start(ctx)
	require.NoError(t, err)
	// registered before the collection cleanups so it runs after them (LIFO)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	t.Run("Replicated", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())
		ds := newDataset(45, 1000, statQueries)
		createSingleVectorCollection(t, "EfReplicated", "hnsw", rqConfig(),
			&models.ReplicationConfig{Factor: 3}, ds, 1000, types.ConsistencyLevelAll)

		// With RF=3 every node holds a replica and the router prefers the
		// local one, so each search runs on the local replica without a
		// node-to-node hop. This proves every node answers consistently and
		// honours ef locally; the cross-node path is pinned by the Sharded
		// subtest.
		assertEFSensitivity(t, compose, ds, "EfReplicated")
	})

	// The Sharded subtest exercises the cross-node ef path. With 3 shards and
	// RF=1 on a 3-node cluster, any node answering a query holds at most one
	// shard locally and must fetch the others from remote nodes through
	// remoteShardSearch -> SearchShard -> postSearchObjects -> IncomingSearch.
	// If a hop dropped the value, the remote shards would be searched with the
	// default beam and the low-ef result would not degrade on them. We assert
	// per node that ef=10 has clearly lower mean recall@10 than ef=1000 and
	// that ef=1000 is near-exact; the handler-level hop is
	// additionally pinned by the clusterapi unit test.
	t.Run("Sharded", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())
		ds := newDataset(46, numObjects, statQueries)
		class := &models.Class{
			Class:             "EfSharded",
			Vectorizer:        "none",
			VectorIndexType:   "hnsw",
			VectorIndexConfig: rqConfig(),
			Properties:        baseProperties(),
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			ShardingConfig:    map[string]any{"desiredCount": 3},
		}
		helper.CreateClass(t, class)
		t.Cleanup(func() { helper.DeleteClass(t, class.Class) })
		insertObjects(t, class.Class, numObjects, types.ConsistencyLevelOne, func(i int) *models.Object {
			return &models.Object{
				ID:     idToUUID(i),
				Class:  class.Class,
				Vector: ds.vectors[i],
				Properties: map[string]any{
					"title":  fmt.Sprintf("object %d", i),
					"bucket": i % numBuckets,
				},
			}
		})
		assertEFSensitivity(t, compose, ds, class.Class)
	})
}

// waitForVectorIndexing blocks until every shard of className reports an empty
// vector queue and a READY index (needed with ASYNC_INDEXING, where inserts
// return before the vectors are indexed).
func waitForVectorIndexing(t *testing.T, className string) {
	t.Helper()
	verbose := "verbose"
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		params := nodes.NewNodesGetClassParams().WithClassName(className).WithOutput(&verbose)
		resp, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
		if !assert.NoError(collect, err) || !assert.NotEmpty(collect, resp.Payload.Nodes) {
			return
		}
		for _, node := range resp.Payload.Nodes {
			for _, shard := range node.Shards {
				assert.Zero(collect, shard.VectorQueueLength, "shard %s still has queued vectors", shard.Name)
				assert.Equal(collect, "READY", shard.VectorIndexingStatus, "shard %s", shard.Name)
			}
		}
	}, 2*time.Minute, time.Second, "vector indexing did not settle for %s", className)
}

// TestVectorSearchParamsEF_Dynamic checks that a dynamic index ignores ef
// while it is flat and honours it once it has upgraded to HNSW. The upgrade
// has no API signal, so the test polls for the behavioural change itself.
func TestVectorSearchParamsEF_Dynamic(t *testing.T) {
	const (
		threshold = 1000
		flatCount = 500
		className = "EfDynamic"
	)
	ctx := context.Background()
	// The dynamic index needs async indexing, which the shared single node
	// does not enable.
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "500ms").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	helper.SetupClient(compose.GetWeaviate().URI())
	c := newGRPC(t, compose.GetWeaviate().GrpcURI())
	ds := newDataset(47, numObjects, statQueries)

	class := &models.Class{
		Class:           className,
		Vectorizer:      "none",
		VectorIndexType: "dynamic",
		VectorIndexConfig: map[string]any{
			"threshold": threshold,
			"hnsw":      rqConfig(),
		},
		Properties: baseProperties(),
	}
	helper.CreateClass(t, class)
	t.Cleanup(func() { helper.DeleteClass(t, className) })

	build := func(i int) *models.Object {
		return &models.Object{
			ID:     idToUUID(i),
			Class:  className,
			Vector: ds.vectors[i],
			Properties: map[string]any{
				"title":  fmt.Sprintf("object %d", i),
				"bucket": i % numBuckets,
			},
		}
	}

	// Stage 1: below the threshold the index is flat and ignores ef.
	insertObjects(t, className, flatCount, types.ConsistencyLevelQuorum, build)
	waitForVectorIndexing(t, className)
	for qi, q := range ds.queries {
		low := searchIDs(t, c, nearVectorRequest(className, q, k, u32(10)))
		high := searchIDs(t, c, nearVectorRequest(className, q, k, u32(1000)))
		require.Len(t, low, k, "query %d", qi)
		require.Equal(t, low, high, "flat stage must ignore ef (query %d)", qi)
	}

	// Stage 2: cross the threshold. The flat->HNSW rebuild runs in the
	// background, so poll until ef shows an effect (or the deadline passes).
	insertObjects(t, className, numObjects-flatCount, types.ConsistencyLevelQuorum,
		func(i int) *models.Object { return build(flatCount + i) })
	waitForVectorIndexing(t, className)

	var high, low float64
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		high = meanRecall(t, c, ds, className, u32(1000), nil, nil)
		low = meanRecall(t, c, ds, className, u32(10), nil, nil)
		assert.LessOrEqual(collect, low, high-0.03, "index has not upgraded to HNSW yet: mean recall ef=10 %.3f, ef=1000 %.3f", low, high)
	}, 3*time.Minute, 5*time.Second, "dynamic index never started honouring ef")
	t.Logf("after upgrade: mean recall@%d ef=1000 %.3f, ef=10 %.3f", k, high, low)
	assert.GreaterOrEqual(t, high, 0.95)
}
