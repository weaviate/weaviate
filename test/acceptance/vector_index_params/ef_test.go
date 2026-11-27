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
	"context"
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
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	dims = 32
	k    = 10
	// numObjects sizes the shared HNSW collections: enough for ef=10 to lose
	// recall against ef=1000 at k=10, small enough to insert in seconds
	// against a race-instrumented server.
	numObjects = 2000
	// numQueries is the query count of the id-by-id equality checks.
	numQueries = 30
	// statQueries is the query count of the mean-recall checks. The asserted
	// gaps (>= 0.03) are above the standard error of a mean recall@10 over 40
	// queries (~0.02).
	statQueries = 40
	numBuckets  = 10
	batchSize   = 500
)

// dataset holds deterministic random vectors; the object at index i has the
// UUID returned by idFor(i) and bucket i % numBuckets. The exact neighbours
// of each query are cached per subset, since the brute-force pass over the
// whole dataset is itself race-instrumented in CI.
type dataset struct {
	vectors [][]float32
	queries [][]float32
	truth   map[truthKey][]string
}

type truthKey struct {
	qi, n  int
	subset string
}

// subset is a deterministic selection of the dataset; its name keys the cache
// of exact neighbours. The zero value keeps every object.
type subset struct {
	name string
	keep func(i int) bool
}

var everything = subset{}

func newDataset(seed int64, n, queries int) *dataset {
	rng := rand.New(rand.NewSource(seed))
	gen := func() []float32 {
		v := make([]float32, dims)
		for j := range v {
			v[j] = rng.Float32()*2 - 1
		}
		return v
	}
	ds := &dataset{truth: map[truthKey][]string{}}
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

// exactTopK returns the ids of the k exact nearest neighbours of query qi by
// cosine distance among the objects of s.
func (d *dataset) exactTopK(qi int, s subset) []string {
	return d.exactTopN(qi, k, s)
}

// exactTopN is exactTopK for an arbitrary result size n.
func (d *dataset) exactTopN(qi, n int, s subset) []string {
	key := truthKey{qi, n, s.name}
	if ids, ok := d.truth[key]; ok {
		return ids
	}
	type scored struct {
		i    int
		dist float64
	}
	q := d.queries[qi]
	var all []scored
	for i, v := range d.vectors {
		if s.keep != nil && !s.keep(i) {
			continue
		}
		all = append(all, scored{i, cosineDistance(q, v)})
	}
	sort.Slice(all, func(a, b int) bool { return all[a].dist < all[b].dist })
	ids := make([]string, 0, n)
	for _, sc := range all[:n] {
		ids = append(ids, idFor(sc.i))
	}
	d.truth[key] = ids
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

// defineSingleVectorCollection creates a legacy single-vector collection and
// loads the dataset into it. The caller owns its deletion.
func defineSingleVectorCollection(t *testing.T, name, indexType string, indexCfg map[string]any,
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

// createSingleVectorCollection is defineSingleVectorCollection for a
// collection owned by one test: it is deleted when the test ends.
func createSingleVectorCollection(t *testing.T, name, indexType string, indexCfg map[string]any,
	repl *models.ReplicationConfig, ds *dataset, n int, cl types.ConsistencyLevel,
) {
	t.Helper()
	defineSingleVectorCollection(t, name, indexType, indexCfg, repl, ds, n, cl)
	t.Cleanup(func() { helper.DeleteClass(t, name) })
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

func efParams(ef *uint32) *pb.VectorIndexParams {
	if ef == nil {
		return nil
	}
	return &pb.VectorIndexParams{Ef: ef}
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
		VectorIndexParams: efParams(ef),
		Uses_127Api:       true,
	}
}

func searchIDs(t *testing.T, c pb.WeaviateClient, req *pb.SearchRequest) []string {
	t.Helper()
	reply, err := c.Search(context.Background(), req)
	return idsOf(t, reply, err)
}

// bucketBelow is the filter keeping the objects whose bucket is < n, i.e.
// n*10% of them, and the matching subset for the exact neighbours.
func bucketBelow(n int) (*pb.Filters, subset) {
	filter := &pb.Filters{
		Operator:  pb.Filters_OPERATOR_LESS_THAN,
		TestValue: &pb.Filters_ValueInt{ValueInt: int64(n)},
		Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "bucket"}},
	}
	return filter, subset{
		name: fmt.Sprintf("bucket<%d", n),
		keep: func(i int) bool { return i%numBuckets < n },
	}
}

func meanRecall(t *testing.T, c pb.WeaviateClient, ds *dataset, collection string, ef *uint32,
	filter *pb.Filters, s subset,
) float64 {
	t.Helper()
	var sum float64
	for qi, q := range ds.queries {
		req := nearVectorRequest(collection, q, k, ef)
		req.Filters = filter
		sum += recallOf(searchIDs(t, c, req), ds.exactTopK(qi, s))
	}
	return sum / float64(len(ds.queries))
}

func TestVectorIndexParamsEF_HNSW(t *testing.T) {
	grpcClient := setupSingleNode(t)
	class, ds := hnswShared.get(t)

	t.Run("unset equals zero", func(t *testing.T) {
		for qi, q := range ds.queries {
			unset := searchIDs(t, grpcClient, nearVectorRequest(class, q, k, nil))
			zero := searchIDs(t, grpcClient, nearVectorRequest(class, q, k, u32(0)))
			require.Equal(t, unset, zero, "query %d", qi)
		}
	})

	t.Run("recall non-decreasing", func(t *testing.T) {
		prev := -1.0
		recalls := map[uint32]float64{}
		for _, ef := range []uint32{10, 200, 1000} {
			r := meanRecall(t, grpcClient, ds, class, u32(ef), nil, everything)
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
		// The beam is floored at the limit, so ef=10 and ef=100 both search
		// with a beam of 100 and must return the same 100 ids.
		for qi, q := range ds.queries[:numQueries] {
			floored := searchIDs(t, grpcClient, nearVectorRequest(class, q, 100, u32(10)))
			require.Len(t, floored, 100, "query %d", qi)
			atLimit := searchIDs(t, grpcClient, nearVectorRequest(class, q, 100, u32(100)))
			require.Equal(t, atLimit, floored, "query %d: ef below limit must behave as ef=limit", qi)
		}
	})

	t.Run("filter below flatSearchCutoff", func(t *testing.T) {
		// With the default flatSearchCutoff (40000) a 10% allow list is
		// brute-forced and ef only caps the compressed candidates kept for
		// rescoring, so the gap is tiny but recall must not get worse with a
		// larger ef. The graph path (flatSearchCutoff 0) is covered per
		// strategy by TestVectorIndexParamsFilterStrategy.
		filter, s := bucketBelow(1)
		low := meanRecall(t, grpcClient, ds, class, u32(10), filter, s)
		high := meanRecall(t, grpcClient, ds, class, u32(1000), filter, s)
		t.Logf("brute force: filtered recall ef=10 %.3f, ef=1000 %.3f", low, high)
		assert.GreaterOrEqual(t, high, 0.95, "ef=1000 must be near-exact")
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
		// The hybrid dense leg fetches max(limit, QUERY_HYBRID_MAXIMUM_RESULTS=100)
		// candidates, so its beam is floored at 100 and an ef below 100 is a
		// no-op. Recall cannot tell beam 100 from beam 1000 at k=100 (both are
		// ~1.0), so the check is exact equality with the equivalent near-vector
		// request, made informative by requiring that the two references (beam
		// 100 and beam 1000) differ on some queries.
		const hybridLimit = 100
		alpha := float32(1)
		hybridIDs := func(q []float32, ef *uint32) []string {
			ids := searchIDs(t, grpcClient, &pb.SearchRequest{
				Collection: class,
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
				VectorIndexParams: efParams(ef),
				Uses_127Api:       true,
			})
			require.Len(t, ids, hybridLimit)
			return ids
		}
		differs := 0
		for qi, q := range ds.queries[:numQueries] {
			narrow := searchIDs(t, grpcClient, nearVectorRequest(class, q, hybridLimit, u32(100)))
			wide := searchIDs(t, grpcClient, nearVectorRequest(class, q, hybridLimit, u32(1000)))
			if !assert.ObjectsAreEqual(narrow, wide) {
				differs++
			}
			// alpha 1: the hybrid result is the dense leg, so it must equal the
			// near-vector request with the same limit and ef...
			require.Equal(t, narrow, hybridIDs(q, u32(100)), "request ef 100, query %d", qi)
			require.Equal(t, wide, hybridIDs(q, u32(1000)), "request ef 1000, query %d", qi)
			// ...and without params both must use the collection default.
			unset := searchIDs(t, grpcClient, nearVectorRequest(class, q, hybridLimit, nil))
			require.Equal(t, unset, hybridIDs(q, nil), "collection ef, query %d", qi)
		}
		t.Logf("hybrid: beam 100 and beam 1000 differ on %d of %d queries", differs, numQueries)
		require.Positive(t, differs, "beam 100 and 1000 never differ, so the equalities above prove nothing")
	})

	t.Run("errors", func(t *testing.T) {
		_, err := grpcClient.Search(context.Background(), nearVectorRequest(class, ds.queries[0], k, u32(10001)))
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
		n      = 300
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
			VectorIndexParams: efParams(u32(ef)),
			Uses_127Api:       true,
		})
		ids := idsOf(t, reply, err)
		require.Len(t, ids, k)
		return ids
	}

	// ef=1000 visits every document (all of them, or all of them after the
	// MUVERA pre-search), so late interaction over that set is the exact
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

func TestVectorIndexParamsEF_REST(t *testing.T) {
	grpcClient := setupSingleNode(t)
	uri := singleNode.GetWeaviate().URI()
	class, ds := hnswShared.get(t)

	tests := []struct {
		name       string
		searchType string
		body       map[string]any
		wantStatus int
	}{
		{"near-vector", "near-vector", map[string]any{"vector": ds.queries[0], "vectorIndexParams": map[string]any{"ef": 800}}, http.StatusOK},
		{"near-object", "near-object", map[string]any{"id": idFor(0), "vectorIndexParams": map[string]any{"ef": 800}}, http.StatusOK},
		{"hybrid", "hybrid", map[string]any{"query": "object", "alpha": 1, "vector": ds.queries[0], "vectorIndexParams": map[string]any{"ef": 800}}, http.StatusOK},
		{"negative ef", "near-vector", map[string]any{"vector": ds.queries[0], "vectorIndexParams": map[string]any{"ef": -1}}, http.StatusUnprocessableEntity},
		{"ef above cap", "near-vector", map[string]any{"vector": ds.queries[0], "vectorIndexParams": map[string]any{"ef": 10001}}, http.StatusUnprocessableEntity},
		{"bm25 rejects params", "bm25", map[string]any{"query": "object", "vectorIndexParams": map[string]any{"ef": 10}}, http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.body["limit"] = k
			status, out := restSearch(t, uri, class, tt.searchType, tt.body)
			require.Equal(t, tt.wantStatus, status, "%v", out)
			if tt.wantStatus == http.StatusOK {
				assert.Len(t, restResultIDs(t, out), k)
			}
		})
	}

	// REST must land on the same search as gRPC: for every query, the ids at
	// ef=10 and at ef=1000 are identical across the two surfaces. The gRPC
	// path is proven ef-sensitive in TestVectorIndexParamsEF_HNSW, so this
	// pins the REST mapping without a second statistical check.
	t.Run("matches gRPC per query", func(t *testing.T) {
		for _, ef := range []uint32{10, 1000} {
			for qi, q := range ds.queries[:numQueries] {
				got := restOKIDs(t, uri, class, "near-vector", map[string]any{
					"vector": q, "limit": k, "vectorIndexParams": map[string]any{"ef": ef},
				})
				want := searchIDs(t, grpcClient, nearVectorRequest(class, q, k, u32(ef)))
				require.Equal(t, want, got, "ef %d query %d", ef, qi)
			}
		}
	})
}

func TestVectorIndexParamsEF_FlatIgnores(t *testing.T) {
	const (
		class = "EfFlat"
		n     = 500
	)
	grpcClient := setupSingleNode(t)
	ds := newDataset(44, n, numQueries)
	createSingleVectorCollection(t, class, "flat", nil, nil, ds, n, types.ConsistencyLevelQuorum)

	for qi, q := range ds.queries {
		with := searchIDs(t, grpcClient, nearVectorRequest(class, q, k, u32(1000)))
		without := searchIDs(t, grpcClient, nearVectorRequest(class, q, k, nil))
		require.Len(t, with, k, "query %d", qi)
		require.Len(t, without, k, "query %d", qi)
		require.Equal(t, without, with, "query %d", qi)
	}
}
