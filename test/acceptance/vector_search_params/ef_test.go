//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io
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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	numBuckets = 10
	batchSize  = 500
)

// dataset holds deterministic random vectors; the object at index i has the
// UUID returned by idFor(i) and bucket i % numBuckets.
type dataset struct {
	vectors [][]float32
	queries [][]float32
}

func newDataset(seed int64, n int) *dataset {
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
	for i := 0; i < numQueries; i++ {
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
	ids := make([]string, 0, k)
	for _, s := range all[:k] {
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
	ds := newDataset(42, numObjects)

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
		for _, ef := range []uint32{10, 50, 200, 1000} {
			r := meanRecall(t, grpcClient, ds, "EfRecall", u32(ef), nil, nil)
			t.Logf("ef=%d recall@%d=%.3f", ef, k, r)
			assert.GreaterOrEqual(t, r, prev, "recall must not decrease when ef grows to %d", ef)
			prev = r
		}
		assert.GreaterOrEqual(t, prev, 0.95, "recall at ef=1000")
	})

	t.Run("ef below limit", func(t *testing.T) {
		createSingleVectorCollection(t, "EfBelowLimit", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)
		for _, q := range ds.queries[:5] {
			ids := searchIDs(t, grpcClient, nearVectorRequest("EfBelowLimit", q, 100, u32(10)))
			require.Len(t, ids, 100)
		}
	})

	t.Run("with filter", func(t *testing.T) {
		createSingleVectorCollection(t, "EfFilter", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)
		filter := &pb.Filters{
			Operator:  pb.Filters_OPERATOR_EQUAL,
			TestValue: &pb.Filters_ValueInt{ValueInt: 3},
			Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "bucket"}},
		}
		keep := func(i int) bool { return i%numBuckets == 3 }
		low := meanRecall(t, grpcClient, ds, "EfFilter", u32(10), filter, keep)
		high := meanRecall(t, grpcClient, ds, "EfFilter", u32(1000), filter, keep)
		t.Logf("filtered recall ef=10: %.3f, ef=1000: %.3f", low, high)
		assert.GreaterOrEqual(t, high, low)
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
		createSingleVectorCollection(t, "EfHybrid", "hnsw", rqConfig(), nil, ds, numObjects, types.ConsistencyLevelQuorum)
		for qi, q := range ds.queries[:10] {
			nv := searchIDs(t, grpcClient, nearVectorRequest("EfHybrid", q, k, u32(1000)))
			alpha := float32(1)
			hybrid := searchIDs(t, grpcClient, &pb.SearchRequest{
				Collection: "EfHybrid",
				Limit:      k,
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
				VectorSearchParams: efParams(u32(1000)),
				Uses_127Api:        true,
			})
			require.Equal(t, nv, hybrid, "query %d", qi)
		}
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

	for qi := 0; qi < 5; qi++ {
		reply, err := c.Search(context.Background(), &pb.SearchRequest{
			Collection: name,
			Limit:      k,
			Metadata:   &pb.MetadataRequest{Uuid: true},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					Type:        pb.Vectors_VECTOR_TYPE_MULTI_FP32,
					VectorBytes: byteops.Fp32SliceOfSlicesToBytes(multi()),
				}},
				Targets: &pb.Targets{TargetVectors: []string{"colbert"}},
			},
			VectorSearchParams: efParams(u32(500)),
			Uses_127Api:        true,
		})
		require.Len(t, idsOf(t, reply, err), k, "query %d", qi)
	}
}

func TestVectorSearchParamsEF_REST(t *testing.T) {
	uri := singleNode.GetWeaviate().URI()
	helper.SetupClient(uri)
	ds := newDataset(43, 1000)
	createSingleVectorCollection(t, "EfREST", "hnsw", rqConfig(), nil, ds, 1000, types.ConsistencyLevelQuorum)

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

	tests := []struct {
		name       string
		searchType string
		params     any
		wantStatus int
		wantHits   int
	}{
		{"ef 800", "near-vector", map[string]any{"ef": 800}, http.StatusOK, k},
		{"negative ef", "near-vector", map[string]any{"ef": -1}, http.StatusUnprocessableEntity, 0},
		{"ef above cap", "near-vector", map[string]any{"ef": 10001}, http.StatusUnprocessableEntity, 0},
		{"bm25 rejects params", "bm25", map[string]any{"ef": 10}, http.StatusUnprocessableEntity, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := map[string]any{"limit": k, "vectorSearchParams": tt.params}
			if tt.searchType == "bm25" {
				body["query"] = "object"
			} else {
				body["vector"] = ds.queries[0]
			}
			status, out := post(tt.searchType, body)
			require.Equal(t, tt.wantStatus, status, "%v", out)
			if tt.wantStatus == http.StatusOK {
				res, ok := out["results"].([]any)
				require.True(t, ok, "%v", out)
				assert.Len(t, res, tt.wantHits)
			}
		})
	}
}

func TestVectorSearchParamsEF_FlatIgnores(t *testing.T) {
	helper.SetupClient(singleNode.GetWeaviate().URI())
	grpcClient := newGRPC(t, singleNode.GetWeaviate().GrpcURI())
	ds := newDataset(44, 1000)
	createSingleVectorCollection(t, "EfFlat", "flat", nil, nil, ds, 1000, types.ConsistencyLevelQuorum)

	for qi, q := range ds.queries {
		with := searchIDs(t, grpcClient, nearVectorRequest("EfFlat", q, k, u32(1000)))
		without := searchIDs(t, grpcClient, nearVectorRequest("EfFlat", q, k, nil))
		require.Equal(t, without, with, "query %d", qi)
	}
}

func TestVectorSearchParamsEF_Replicated(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().WithWeaviateClusterWithGRPC().Start(ctx)
	require.NoError(t, err)
	// registered before the collection cleanup so it runs after it (LIFO)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	helper.SetupClient(compose.GetWeaviate().URI())
	ds := newDataset(45, 1000)
	createSingleVectorCollection(t, "EfReplicated", "hnsw", rqConfig(),
		&models.ReplicationConfig{Factor: 3}, ds, 1000, types.ConsistencyLevelAll)

	for node := 1; node <= 3; node++ {
		t.Run(fmt.Sprintf("node %d", node), func(t *testing.T) {
			c := newGRPC(t, compose.GetWeaviateNode(node).GrpcURI())
			matches := 0
			for _, q := range ds.queries {
				got := searchIDs(t, c, nearVectorRequest("EfReplicated", q, k, u32(1000)))
				if recallOf(got, ds.exactTopK(q, nil)) == 1 {
					matches++
				}
			}
			t.Logf("node %d: %d/%d queries match brute force", node, matches, len(ds.queries))
			assert.GreaterOrEqual(t, matches, 28)
		})
	}
}
