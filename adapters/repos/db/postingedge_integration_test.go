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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/postingedge"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex"
	postingedgeent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

const postingEdgeTestClass = "PostingEdgeClass"

// postingEdgeCorpus is the vectors of the end-to-end test and the object each
// one is the vector of. They have neighbors at every distance, as embeddings
// do: points of a space of a few dimensions laid in the space of the vectors
// by a random linear map, with a little noise.
type postingEdgeCorpus struct {
	ids     []strfmt.UUID
	vectors [][]float32
	// normalized has the vectors as cosine compares them.
	normalized [][]float32
	queries    [][]float32
}

// newPostingEdgeCorpus draws n vectors and 50 queries. Two corpora of the
// same lay and different draws are of the same space, and hold other vectors.
func newPostingEdgeCorpus(n, dims int, draws uint64) *postingEdgeCorpus {
	const latent = 8
	layRNG := rand.New(rand.NewPCG(20261120, 1))
	lay := make([][]float32, dims)
	for d := range lay {
		lay[d] = make([]float32, latent)
		for j := range lay[d] {
			lay[d][j] = float32(layRNG.NormFloat64())
		}
	}
	rng := rand.New(rand.NewPCG(20261120, draws))
	point := make([]float32, latent)
	draw := func() []float32 {
		for j := range point {
			point[j] = float32(rng.NormFloat64())
		}
		vector := make([]float32, dims)
		for d := range vector {
			var x float32
			for j, p := range point {
				x += lay[d][j] * p
			}
			vector[d] = x + 0.05*float32(rng.NormFloat64())
		}
		return vector
	}
	c := &postingEdgeCorpus{}
	for range n {
		vector := draw()
		c.ids = append(c.ids, strfmt.UUID(uuid.NewString()))
		c.vectors = append(c.vectors, vector)
		c.normalized = append(c.normalized, distancer.Normalize(vector))
	}
	for range 50 {
		c.queries = append(c.queries, draw())
	}
	return c
}

// nearest returns the ids of the k objects nearest to a query by brute
// force, among those that held gives.
func (c *postingEdgeCorpus) nearest(t *testing.T, query []float32, k int, held func(i int) bool) []strfmt.UUID {
	t.Helper()
	provider := distancer.NewCosineDistanceProvider()
	query = distancer.Normalize(query)
	type scored struct {
		i    int
		dist float32
	}
	var all []scored
	for i, vector := range c.normalized {
		if !held(i) {
			continue
		}
		dist, err := provider.SingleDist(query, vector)
		require.NoError(t, err)
		all = append(all, scored{i, dist})
	}
	slices.SortFunc(all, func(a, b scored) int {
		switch {
		case a.dist < b.dist:
			return -1
		case a.dist > b.dist:
			return 1
		}
		return a.i - b.i
	})
	out := make([]strfmt.UUID, 0, k)
	for _, s := range all[:min(k, len(all))] {
		out = append(out, c.ids[s.i])
	}
	return out
}

func postingEdgeTestClassOf(t *testing.T, config map[string]interface{}) *models.Class {
	t.Helper()
	// The config goes through the parser a class goes through when it is
	// created.
	parsed, err := vectorindex.ParseAndValidateConfig(config, vectorindex.VectorIndexTypePostingEdge, false)
	require.NoError(t, err)
	return &models.Class{
		Class:               postingEdgeTestClass,
		VectorIndexType:     vectorindex.VectorIndexTypePostingEdge,
		VectorIndexConfig:   parsed,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:         "name",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord,
		}},
	}
}

func postingEdgeTestShard(t *testing.T, repo *DB) *Shard {
	t.Helper()
	var shard *Shard
	require.NoError(t, repo.GetIndex(postingEdgeTestClass).ForEachShard(func(_ string, s ShardLike) error {
		loaded, ok := s.(*Shard)
		require.True(t, ok, "a shard of %T: the test opens its shards at once", s)
		shard = loaded
		return nil
	}))
	require.NotNil(t, shard)
	return shard
}

// The posting-edge index end to end, through a shard: a class is created
// with it, objects are imported, their neighbors are found close to what
// brute force finds, a part of them is deleted and no longer found, some are
// updated and found by their new vector, and after a restart everything is
// as it was. What the index does not do fails the query with an error that
// says what.
func TestPostingEdge_ThroughAShard(t *testing.T) {
	const (
		dims  = 64
		base  = 1000
		total = 3200
		k     = 10
	)
	// Not the context of the other tests of the package, which ends after
	// thirty seconds: under the race detector the whole of this takes longer.
	ctx := context.Background()
	corpus := newPostingEdgeCorpus(total, dims, 2)
	class := postingEdgeTestClassOf(t, map[string]interface{}{
		"distance":         "cosine",
		"ef":               float64(64),
		"efConstruction":   float64(160),
		"maxPostingSizeKB": float64(8),
		"rq": map[string]interface{}{
			"rescoreLimit": float64(100),
			"cacheMode":    "routing",
		},
	})
	dir, shardState := t.TempDir(), singleShardState()
	open := func() *DB {
		return openTestDatabase(t, dir, shardState, monitoring.GetMetrics(), func(config *Config) {
			config.EnableLazyLoadShards = boolPtr(false)
			// The index gathers this many vectors before it builds its first
			// postings: the default is ten times as many.
			config.PostingEdgeBaseSize = base
		}, nil, class)
	}
	repo := open()
	closed := false
	defer func() {
		if !closed {
			require.NoError(t, repo.Shutdown(context.Background()))
		}
	}()

	// deleted has the objects that are gone, and replaced the vector an
	// object was updated to, in place of its own.
	deleted := map[int]bool{}
	held := func(i int) bool { return !deleted[i] }

	search := func(t *testing.T, repo *DB, query []float32, limit int) []strfmt.UUID {
		t.Helper()
		res, err := repo.VectorSearch(ctx, dto.GetParams{
			ClassName:  postingEdgeTestClass,
			Pagination: &filters.Pagination{Limit: limit},
		}, []string{""}, []models.Vector{query})
		require.NoError(t, err)
		ids := make([]strfmt.UUID, len(res))
		for i, r := range res {
			ids[i] = r.ID
		}
		return ids
	}
	recall := func(t *testing.T, repo *DB, upTo int) float64 {
		t.Helper()
		hits, wanted := 0, 0
		for _, query := range corpus.queries {
			got := search(t, repo, query, k)
			want := corpus.nearest(t, query, k, func(i int) bool { return i < upTo && held(i) })
			for _, id := range got {
				at := slices.Index(corpus.ids, id)
				require.GreaterOrEqual(t, at, 0, "a result that was never imported")
				require.True(t, at < upTo && held(at), "object %d was returned, and the class does not hold it", at)
			}
			for _, id := range want {
				if slices.Contains(got, id) {
					hits++
				}
			}
			wanted += len(want)
		}
		return float64(hits) / float64(wanted)
	}
	importRange := func(t *testing.T, repo *DB, from, to int) {
		t.Helper()
		const batchSize = 100
		for start := from; start < to; start += batchSize {
			end := min(start+batchSize, to)
			batch := make(objects.BatchObjects, 0, end-start)
			for i := start; i < end; i++ {
				batch = append(batch, objects.BatchObject{
					OriginalIndex: i - start,
					Object: &models.Object{
						Class:      postingEdgeTestClass,
						ID:         corpus.ids[i],
						Properties: map[string]interface{}{"name": fmt.Sprintf("object %d", i)},
						Vector:     corpus.vectors[i],
					},
					UUID: corpus.ids[i],
				})
			}
			res, err := repo.BatchPutObjects(ctx, batch, nil, 0)
			require.NoError(t, err)
			for _, object := range res {
				require.NoError(t, object.Err, "object %d", start+object.OriginalIndex)
			}
		}
	}
	index := func(t *testing.T, repo *DB) *postingedge.Persistent {
		t.Helper()
		var index *postingedge.Persistent
		require.NoError(t, postingEdgeTestShard(t, repo).ForEachVectorIndex(func(_ string, vectorIndex VectorIndex) error {
			found, isPostingEdge := vectorIndex.(*postingedge.Persistent)
			require.True(t, isPostingEdge, "the vector index of the class is a %T", vectorIndex)
			index = found
			return nil
		}))
		require.NotNil(t, index)
		return index
	}

	t.Run("an empty class finds nothing", func(t *testing.T) {
		require.Equal(t, common.IndexType(common.IndexTypePostingEdge), index(t, repo).Type())
		assert.Empty(t, search(t, repo, corpus.queries[0], k))
	})

	t.Run("the first objects are found exactly", func(t *testing.T) {
		importRange(t, repo, 0, base/2)
		assert.Equal(t, 1.0, recall(t, repo, base/2))
	})

	t.Run("imported, the objects are found close to brute force", func(t *testing.T) {
		importRange(t, repo, base/2, total)
		got := recall(t, repo, total)
		t.Logf("recall@%d after importing %d objects: %.4f", k, total, got)
		assert.Greater(t, got, 0.9)
		meta := filepath.Join(postingEdgeTestShard(t, repo).path(),
			helpers.PostingEdgeDirName(helpers.VectorIndexIDForTarget("")), "meta.json")
		_, err := os.Stat(meta)
		assert.NoError(t, err, "past %d objects the index has built its postings", base)
	})

	t.Run("deleted, an object is not found", func(t *testing.T) {
		for i := 0; i < total; i += 4 {
			require.NoError(t, repo.DeleteObject(ctx, postingEdgeTestClass, corpus.ids[i], time.Time{}, nil, "", 0))
			deleted[i] = true
		}
		got := recall(t, repo, total)
		t.Logf("recall@%d with a quarter of the objects deleted: %.4f", k, got)
		assert.Greater(t, got, 0.9)
	})

	t.Run("updated, an object is found by its new vector", func(t *testing.T) {
		// Objects 1, 5, 9, ... take the vector of a query, far from their
		// own: the shard deletes the old vector and adds the new one.
		for q, query := range corpus.queries[:10] {
			i := 1 + 4*q
			require.NoError(t, repo.PutObject(ctx, &models.Object{
				Class:      postingEdgeTestClass,
				ID:         corpus.ids[i],
				Properties: map[string]interface{}{"name": fmt.Sprintf("object %d, updated", i)},
			}, query, nil, nil, nil, 0))
			corpus.vectors[i], corpus.normalized[i] = query, distancer.Normalize(query)
			got := search(t, repo, query, 1)
			require.Equal(t, []strfmt.UUID{corpus.ids[i]}, got, "object %d is the nearest to the vector it was given", i)
		}
		assert.Greater(t, recall(t, repo, total), 0.9)
	})

	before := make([][]strfmt.UUID, len(corpus.queries))
	t.Run("restarted, the class is as it was", func(t *testing.T) {
		// What the index does in the background with the deleted vectors and
		// with its own vectors is done before its answers are taken.
		require.NoError(t, index(t, repo).Settle(ctx))
		for i, query := range corpus.queries {
			before[i] = search(t, repo, query, k)
		}
		require.NoError(t, repo.Shutdown(context.Background()))
		closed = true
		repo = open()
		closed = false

		for i, query := range corpus.queries {
			require.Equal(t, before[i], search(t, repo, query, k), "query %d", i)
		}
		got := recall(t, repo, total)
		t.Logf("recall@%d after the restart: %.4f", k, got)
		assert.Greater(t, got, 0.9)
	})

	t.Run("after the restart it goes on", func(t *testing.T) {
		more := newPostingEdgeCorpus(300, dims, 3)
		for i, vector := range more.vectors {
			require.NoError(t, repo.PutObject(ctx, &models.Object{
				Class: postingEdgeTestClass, ID: more.ids[i],
				Properties: map[string]interface{}{"name": "late"},
			}, vector, nil, nil, nil, 0))
		}
		for i, vector := range more.vectors[:20] {
			require.Equal(t, []strfmt.UUID{more.ids[i]}, search(t, repo, vector, 1), "a late object is its own nearest")
		}
		require.NoError(t, repo.DeleteObject(ctx, postingEdgeTestClass, more.ids[0], time.Time{}, nil, "", 0))
		require.NotContains(t, search(t, repo, more.vectors[0], k), more.ids[0])
	})

	t.Run("what the index does not do fails the query", func(t *testing.T) {
		shard := postingEdgeTestShard(t, repo)
		filter := &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On:       &filters.Path{Class: postingEdgeTestClass, Property: "name"},
			Value:    &filters.Value{Value: "late", Type: schema.DataTypeText},
		}}
		_, err := repo.VectorSearch(ctx, dto.GetParams{
			ClassName:  postingEdgeTestClass,
			Pagination: &filters.Pagination{Limit: k},
			Filters:    filter,
		}, []string{""}, []models.Vector{corpus.queries[0]})
		require.ErrorContains(t, err, "filtered search")
		require.ErrorContains(t, err, "not supported")

		_, _, err = shard.ObjectVectorSearch(ctx, []models.Vector{corpus.queries[0]}, []string{""}, 0.5, -1,
			nil, nil, nil, additional.Properties{}, nil, nil)
		require.ErrorContains(t, err, "search by distance")
		require.ErrorContains(t, err, "not supported")

		// And a query the index does take still works after those.
		assert.Len(t, search(t, repo, corpus.queries[0], k), k)
	})
}

// A class cannot be created with what the index does not have: the parser of
// its config refuses it, so no shard ever opens such an index.
func TestPostingEdge_ClassConfig(t *testing.T) {
	rq := func(key string, value interface{}) map[string]interface{} {
		return map[string]interface{}{"rq": map[string]interface{}{key: value}}
	}
	tests := []struct {
		name        string
		config      map[string]interface{}
		multiVector bool
		wantErr     string
	}{
		{name: "nothing: the defaults"},
		{name: "everything there is", config: map[string]interface{}{
			"distance": "cosine", "ef": float64(40), "efConstruction": float64(200), "maxPostingSizeKB": float64(76),
			"rq": map[string]interface{}{"rescoreLimit": float64(200), "cacheMode": "routing"},
		}},
		{name: "l2-squared", config: map[string]interface{}{"distance": "l2-squared"}, wantErr: "unsupported distance"},
		{name: "dot", config: map[string]interface{}{"distance": "dot"}, wantErr: "unsupported distance"},
		{name: "manhattan", config: map[string]interface{}{"distance": "manhattan"}, wantErr: "unsupported distance"},
		{name: "hamming", config: map[string]interface{}{"distance": "hamming"}, wantErr: "unsupported distance"},
		{name: "the cache mode that is not there yet", config: rq("cacheMode", "none"), wantErr: "not available yet"},
		{name: "a cache mode there is not", config: rq("cacheMode", "scan"), wantErr: "unknown rq.cacheMode"},
		{name: "a boolean for a cache mode", config: rq("cacheMode", true), wantErr: "unknown rq.cacheMode"},
		{name: "multi-vectors", multiVector: true, wantErr: "multi-vectors are not supported"},
		{name: "pq", config: map[string]interface{}{"pq": map[string]interface{}{"enabled": true}}, wantErr: "pq is not supported"},
		{name: "bq", config: map[string]interface{}{"bq": map[string]interface{}{"enabled": true}}, wantErr: "bq is not supported"},
		{name: "sq", config: map[string]interface{}{"sq": map[string]interface{}{"enabled": true}}, wantErr: "sq is not supported"},
		{name: "rq with 8 bits", config: rq("bits", float64(8)), wantErr: "1 bit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input interface{}
			if tt.config != nil {
				input = tt.config
			}
			parsed, err := vectorindex.ParseAndValidateConfig(input, vectorindex.VectorIndexTypePostingEdge, tt.multiVector)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, vectorindex.VectorIndexTypePostingEdge, parsed.IndexType())
			require.Equal(t, "cosine", parsed.DistanceName())
			require.False(t, parsed.IsMultiVector())
		})
	}

	// Whatever gets past the parser some other way, the shard does not open
	// an index for a distance it does not have.
	t.Run("a shard refuses another distance", func(t *testing.T) {
		for _, provider := range []distancer.Provider{distancer.NewL2SquaredProvider(), distancer.NewDotProductProvider()} {
			_, err := postingedge.Open(postingedge.PersistentConfig{
				ID: "main", RootPath: t.TempDir(), DistanceProvider: provider,
			}, postingedgeent.NewDefaultUserConfig(), nil)
			require.ErrorIs(t, err, postingedge.ErrNotSupported, provider.Type())
		}
	})
}

// Two named vectors of a class, each with a posting-edge index of its own in
// the same shard: their buckets and their directories carry the id of their
// index, so neither answers with the vectors of the other, before a restart
// or after it.
func TestPostingEdge_NamedVectors(t *testing.T) {
	const (
		dims  = 64
		base  = 800
		total = 2000
		k     = 10
	)
	// Not the context of the other tests of the package, which ends after
	// thirty seconds: under the race detector the whole of this takes longer.
	ctx := context.Background()
	names := []string{"title", "body"}
	// The same objects have a vector in each, drawn apart.
	corpora := map[string]*postingEdgeCorpus{
		"title": newPostingEdgeCorpus(total, dims, 11),
		"body":  newPostingEdgeCorpus(total, dims, 12),
	}
	ids := corpora["title"].ids
	corpora["body"].ids = ids

	parsed, err := vectorindex.ParseAndValidateConfig(map[string]interface{}{
		"ef": float64(64), "maxPostingSizeKB": float64(8), "rq": map[string]interface{}{"rescoreLimit": float64(100)},
	}, vectorindex.VectorIndexTypePostingEdge, false)
	require.NoError(t, err)
	vectorConfig := map[string]models.VectorConfig{}
	for _, name := range names {
		vectorConfig[name] = models.VectorConfig{
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			VectorIndexType:   vectorindex.VectorIndexTypePostingEdge,
			VectorIndexConfig: parsed,
		}
	}
	class := &models.Class{
		Class:               postingEdgeTestClass,
		InvertedIndexConfig: invertedConfig(),
		VectorConfig:        vectorConfig,
		Properties: []*models.Property{{
			Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord,
		}},
	}
	dir, shardState := t.TempDir(), singleShardState()
	open := func() *DB {
		return openTestDatabase(t, dir, shardState, monitoring.GetMetrics(), func(config *Config) {
			config.EnableLazyLoadShards = boolPtr(false)
			config.PostingEdgeBaseSize = base
		}, nil, class)
	}
	repo := open()
	closed := false
	defer func() {
		if !closed {
			require.NoError(t, repo.Shutdown(context.Background()))
		}
	}()

	deleted := map[int]bool{}
	answers := func(t *testing.T, repo *DB, name string) ([][]strfmt.UUID, float64) {
		t.Helper()
		corpus := corpora[name]
		out := make([][]strfmt.UUID, len(corpus.queries))
		hits, wanted := 0, 0
		for q, query := range corpus.queries {
			res, err := repo.VectorSearch(ctx, dto.GetParams{
				ClassName:  postingEdgeTestClass,
				Pagination: &filters.Pagination{Limit: k},
			}, []string{name}, []models.Vector{query})
			require.NoError(t, err)
			for _, r := range res {
				at := slices.Index(ids, r.ID)
				require.GreaterOrEqual(t, at, 0)
				require.False(t, deleted[at], "%s: object %d was returned after it was deleted", name, at)
				out[q] = append(out[q], r.ID)
			}
			want := corpus.nearest(t, query, k, func(i int) bool { return !deleted[i] })
			for _, id := range want {
				if slices.Contains(out[q], id) {
					hits++
				}
			}
			wanted += len(want)
		}
		return out, float64(hits) / float64(wanted)
	}

	const batchSize = 100
	for start := 0; start < total; start += batchSize {
		batch := make(objects.BatchObjects, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			batch = append(batch, objects.BatchObject{
				OriginalIndex: i - start,
				Object: &models.Object{
					Class:      postingEdgeTestClass,
					ID:         ids[i],
					Properties: map[string]interface{}{"name": fmt.Sprintf("object %d", i)},
					Vectors: models.Vectors{
						"title": corpora["title"].vectors[i],
						"body":  corpora["body"].vectors[i],
					},
				},
				UUID: ids[i],
			})
		}
		res, err := repo.BatchPutObjects(ctx, batch, nil, 0)
		require.NoError(t, err)
		for _, object := range res {
			require.NoError(t, object.Err, "object %d", start+object.OriginalIndex)
		}
	}
	for i := 0; i < total; i += 5 {
		require.NoError(t, repo.DeleteObject(ctx, postingEdgeTestClass, ids[i], time.Time{}, nil, "", 0))
		deleted[i] = true
	}

	// Each index has its own directory and its own buckets in the store.
	shardPath := postingEdgeTestShard(t, repo).path()
	for _, name := range names {
		id := helpers.VectorIndexIDForTarget(name)
		_, err := os.Stat(filepath.Join(shardPath, helpers.PostingEdgeDirName(id), "meta.json"))
		require.NoError(t, err, "the index of %q has built its postings", name)
		staging := helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, id)
		for _, bucket := range helpers.PostingEdgeBucketNames(id) {
			_, err := os.Stat(filepath.Join(shardPath, "lsm", bucket))
			if bucket == staging {
				// The vectors it kept aside went with their bucket once the
				// postings were built.
				require.True(t, os.IsNotExist(err), "bucket %s", bucket)
				continue
			}
			require.NoError(t, err, "bucket %s", bucket)
		}
	}

	// What each index does in the background is done before its answers are
	// taken.
	require.NoError(t, postingEdgeTestShard(t, repo).ForEachVectorIndex(func(name string, vectorIndex VectorIndex) error {
		found, isPostingEdge := vectorIndex.(*postingedge.Persistent)
		require.True(t, isPostingEdge, "the vector index %q is a %T", name, vectorIndex)
		return found.Settle(ctx)
	}))
	before := map[string][][]strfmt.UUID{}
	for _, name := range names {
		got, recall := answers(t, repo, name)
		t.Logf("%s: recall@%d %.4f", name, k, recall)
		assert.Greater(t, recall, 0.9, name)
		before[name] = got
	}

	require.NoError(t, repo.Shutdown(context.Background()))
	closed = true
	repo = open()
	closed = false
	for _, name := range names {
		got, recall := answers(t, repo, name)
		require.Equal(t, before[name], got, "%s: the same answers after a restart", name)
		assert.Greater(t, recall, 0.9, name)
	}
}
