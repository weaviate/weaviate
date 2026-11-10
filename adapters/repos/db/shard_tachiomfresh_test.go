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
	stderrors "errors"
	"io/fs"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/tachiomfresh"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	tachiomfreshent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
	"github.com/weaviate/weaviate/usecases/objects"
)

// tachiomfreshTestTokenIDs gives the tokens of a test document their token
// ids, three ids by position.
func tachiomfreshTestTokenIDs(tokens int) []uint32 {
	ids := make([]uint32, tokens)
	for i := range ids {
		ids[i] = uint32(i % 3)
	}
	return ids
}

// tachiomfreshTestDocs builds n documents of 3 to 6 random tokens of dims
// dimensions, seeded.
func tachiomfreshTestDocs(rng *rand.Rand, n, dims int) [][][]float32 {
	docs := make([][][]float32, n)
	for i := range docs {
		tokens := 3 + rng.Intn(4)
		docs[i] = make([][]float32, tokens)
		for j := range docs[i] {
			docs[i][j] = make([]float32, dims)
			for d := range docs[i][j] {
				docs[i][j][d] = rng.Float32()*2 - 1
			}
		}
	}
	return docs
}

// docIDsOf reads back the doc id the shard gave every object: a batch put
// writes its objects concurrently, so the ids do not follow the list.
func docIDsOf(t *testing.T, ctx context.Context, shd ShardLike, objs []*storobj.Object) []uint64 {
	t.Helper()
	ids := make([]uint64, len(objs))
	for i, obj := range objs {
		stored, err := shd.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
		require.NoError(t, err)
		require.NotNil(t, stored)
		ids[i] = stored.DocID
	}
	return ids
}

// dotMaxSim is the brute-force MaxSim distance the index computes: the sum
// over query tokens of the minimum dot distance (-dot) to a document token.
func dotMaxSim(query, doc [][]float32) float32 {
	var score float64
	for _, q := range query {
		best := math.MaxFloat64
		for _, d := range doc {
			var dot float64
			for i := range q {
				dot += float64(q[i]) * float64(d[i])
			}
			if -dot < best {
				best = -dot
			}
		}
		score += best
	}
	return float32(score)
}

// bruteForceTop returns the k live documents with the lowest MaxSim
// distance to query, best first.
func bruteForceTop(query [][]float32, docs map[uint64][][]float32, live func(docID uint64) bool, k int) []uint64 {
	type scored struct {
		id   uint64
		dist float32
	}
	var all []scored
	for id, doc := range docs {
		if !live(id) {
			continue
		}
		all = append(all, scored{id, dotMaxSim(query, doc)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].dist != all[j].dist {
			return all[i].dist < all[j].dist
		}
		return all[i].id < all[j].id
	})
	ids := make([]uint64, 0, k)
	for i := 0; i < len(all) && i < k; i++ {
		ids = append(ids, all[i].id)
	}
	return ids
}

// TestShard_TachiomFresh: a class whose named multi-vector target is a
// tachiomfresh index, through a shard, under the 10,000-token threshold
// (the index stages its documents) and above it (the index is built), on a
// single-tenant and a multi-tenant shard. The documents go in through
// PutObjectBatch. With budgets that probe every centroid and score every
// document, every search equals the brute force over the live documents.
// The journeys: the build, unfiltered and filtered search (the flat path
// and the widening), a batch into the built index, an update under the
// same uuid, a delete and a re-insert, deletes, a restart through
// initShard, the backup listing and the drop of the vector index.
func TestShard_TachiomFresh(t *testing.T) {
	t.Setenv("ASYNC_INDEXING_STALE_TIMEOUT", "200ms")
	tests := []struct {
		name        string
		amount      int
		built       bool
		multiTenant bool
	}{
		{name: "staged", amount: 300},
		{name: "built", amount: 2600, built: true},
		{name: "built, multi-tenant", amount: 2600, built: true, multiTenant: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testShardTachiomFresh(t, tt.amount, tt.built, tt.multiTenant)
		})
	}
}

func testShardTachiomFresh(t *testing.T, amount int, built, multiTenant bool) {
	ctx := context.Background()
	const (
		className = "TachiomFreshClass"
		target    = "colbert"
		dims      = 8
		k         = 10
		// documents added to the index once it holds amount
		extra = 300
		// the documents of group 1, which the widening's filter allows,
		// and the objects without a multi-vector it also allows
		widenDocs    = 20
		vectorless   = 1100
		widenKDocs   = 500
		buildTokens  = 10_000
		maxWidenings = 4
	)
	rng := rand.New(rand.NewSource(42))
	docs := tachiomfreshTestDocs(rng, amount, dims)
	queries := tachiomfreshTestDocs(rng, 5, dims)
	// the documents of group 1 lie near (-1, ..., -1) and the widening's
	// queries near (1, ..., 1): their centroids have the lowest dot product
	// with the query, so a probe of a few centroids does not reach them
	near := func(docs [][][]float32, at float32) {
		for _, doc := range docs {
			for _, token := range doc {
				for d := range token {
					token[d] = at + (rng.Float32()-0.5)/10
				}
			}
		}
	}
	near(docs[:widenDocs], -1)
	widenQueries := tachiomfreshTestDocs(rng, 5, dims)
	near(widenQueries, 1)
	var tokens int
	for _, doc := range docs {
		tokens += len(doc)
	}
	require.Equal(t, built, tokens >= buildTokens, "%d tokens", tokens)

	// a small posting size so that the index splits, and budgets that probe
	// every centroid and score every document, so the search is exact
	uc := tachiomfreshent.NewDefaultUserConfig()
	uc.MaxPostingSize = 16
	uc.KCentroids = 4096
	uc.KDocsToScore = uint32(amount + extra)
	// the int property "n" (the object's number) is what the flat filter
	// filters on, "g" what the widening's filter does
	class := &models.Class{
		Class:               className,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{DataType: []string{string(schema.DataTypeInt)}, Name: "n"},
			{DataType: []string{string(schema.DataTypeInt)}, Name: "g"},
		},
		VectorConfig: map[string]models.VectorConfig{target: {
			VectorIndexType:   "tachiomfresh",
			VectorIndexConfig: uc,
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
		}},
	}
	// the named vector only, as a class with named vectors has: no
	// class-level vector config, so the shard opens no legacy index
	idxOpt := func(i *Index) {
		i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{target: uc}
	}
	var (
		shd ShardLike
		idx *Index
	)
	if multiTenant {
		shd, idx = testShardWithMultiTenantSettings(t, ctx, class, nil, false, true, idxOpt)
	} else {
		shd, idx = testShardWithSettings(t, ctx, class, nil, false, true, idxOpt)
	}
	defer func() {
		require.NoError(t, idx.drop())
	}()

	newObject := func(n int, doc [][]float32) *storobj.Object {
		obj := testObject(className)
		g := int64(0)
		if n < widenDocs {
			g = 1
		}
		obj.Object.Properties = map[string]interface{}{"n": int64(n), "g": g}
		obj.MultiVectors = map[string][][]float32{target: doc}
		obj.Object.TokenIds = models.TokenIds{target: tachiomfreshTestTokenIDs(len(doc))}
		return obj
	}
	objs := make([]*storobj.Object, amount)
	for i := range objs {
		objs[i] = newObject(i, docs[i])
	}
	for _, err := range shd.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}
	if built {
		// objects of group 1 without the multi-vector: the widening's
		// filter allows them, the index does not hold them
		plain := make([]*storobj.Object, vectorless)
		for i := range plain {
			plain[i] = testObject(className)
			plain[i].Object.Properties = map[string]interface{}{"n": int64(1_000_000 + i), "g": int64(1)}
		}
		for _, err := range shd.PutObjectBatch(ctx, plain) {
			require.NoError(t, err)
		}
	}

	// the model the searches are checked against: the doc id of every
	// object, the tokens under every doc id the index was given, and the
	// doc ids it no longer holds
	uuidToDoc := map[strfmt.UUID]uint64{}
	docsByID := map[uint64][][]float32{}
	nByID := map[uint64]int{}
	deleted := map[uint64]bool{}
	record := func(t *testing.T, objs []*storobj.Object) {
		for i, id := range docIDsOf(t, ctx, shd, objs) {
			if old, ok := uuidToDoc[objs[i].ID()]; ok && old != id {
				deleted[old] = true
			}
			delete(deleted, id)
			uuidToDoc[objs[i].ID()] = id
			docsByID[id] = objs[i].MultiVectors[target]
			nByID[id] = int(objs[i].Object.Properties.(map[string]interface{})["n"].(int64))
		}
	}
	record(t, objs)
	require.Len(t, docsByID, amount)
	live := func(id uint64) bool { return !deleted[id] }

	waitForQueue := func(t *testing.T, shd ShardLike) {
		_, q := getVectorIndexAndQueue(t, shd, target)
		for i := 0; i < 100 && q.Size() != 0; i++ {
			time.Sleep(200 * time.Millisecond)
		}
		require.Zero(t, q.Size(), "the index queue did not drain")
	}
	waitForQueue(t, shd)

	search := func(t *testing.T, shd ShardLike, query [][]float32, filter *filters.LocalFilter) []uint64 {
		found, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{query}, []string{target},
			0, k, filter, nil, nil, additional.Properties{}, nil, nil)
		require.NoError(t, err)
		got := make([]uint64, len(found))
		for i, obj := range found {
			id, ok := uuidToDoc[obj.ID()]
			require.True(t, ok, "an unknown object")
			got[i] = id
		}
		return got
	}
	checkSearch := func(t *testing.T, shd ShardLike) {
		for qi, query := range queries {
			require.Equal(t, bruteForceTop(query, docsByID, live, k), search(t, shd, query, nil), "query %d", qi)
		}
	}
	intFilter := func(op filters.Operator, property string, value int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: op,
			On:       &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(property)},
			Value:    &filters.Value{Value: value, Type: schema.DataTypeInt},
		}}
	}
	// n < 120 allows at most 120 documents, under 2 x kDocsToScore: the flat
	// path, the exact scoring of every allowed document (the staged index
	// scores every document anyway)
	const below = 120
	checkFiltered := func(t *testing.T, shd ShardLike) {
		allowed := func(id uint64) bool { return live(id) && nByID[id] < below }
		for qi, query := range queries {
			require.Equal(t, bruteForceTop(query, docsByID, allowed, k),
				search(t, shd, query, intFilter(filters.OperatorLessThan, "n", below)), "filtered query %d", qi)
		}
	}
	checkContains := func(t *testing.T, shd ShardLike) {
		vidx, _ := getVectorIndexAndQueue(t, shd, target)
		for id := range docsByID {
			require.Equal(t, live(id), vidx.ContainsDoc(id), "doc %d", id)
		}
		require.False(t, vidx.ContainsDoc(1<<30), "never inserted")
		var iterated int
		vidx.Iterate(func(id uint64) bool {
			require.True(t, live(id), "iterate returned doc %d, not live", id)
			iterated++
			return true
		})
		require.Equal(t, len(docsByID)-len(deleted), iterated)
	}
	checkAll := func(t *testing.T, shd ShardLike) {
		checkSearch(t, shd)
		checkFiltered(t, shd)
		checkContains(t, shd)
	}
	tachiom := func(t *testing.T, shd ShardLike) *tachiomfresh.TachiomFresh {
		vidx, _ := getVectorIndexAndQueue(t, shd, target)
		tf, ok := vidx.(*tachiomfresh.TachiomFresh)
		require.True(t, ok, "%T", vidx)
		return tf
	}
	// the number of live centroids: a search of the centroid graph for
	// more than it holds returns all of them
	countCentroids := func(t *testing.T, shd ShardLike) int {
		const most = 1 << 14
		res, err := tachiom(t, shd).Centroids.Search(ctx, make([]float32, dims), most, nil)
		require.NoError(t, err)
		require.Less(t, res.Len(), most)
		return res.Len()
	}
	bucketKeys := func(t *testing.T, shd ShardLike, name string) int {
		bucket := loadedTachiomShard(t, ctx, shd).store.Bucket(name)
		require.NotNil(t, bucket, name)
		c := bucket.Cursor()
		defer c.Close()
		var n int
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			n++
		}
		return n
	}
	indexID := "vectors_" + target

	t.Run("the index is built or staged", func(t *testing.T) {
		staged := bucketKeys(t, shd, helpers.TachiomFreshStagingBucketName(indexID))
		if !built {
			require.Equal(t, amount, staged, "every document is staged")
			require.Zero(t, countCentroids(t, shd))
			return
		}
		require.Zero(t, staged, "the build empties the staging bucket")
		// three token types, each starting with one centroid; more than
		// four per type means splits ran on the shard's readers
		require.Greater(t, countCentroids(t, shd), 3*4)
	})

	t.Run("search", func(t *testing.T) {
		checkAll(t, shd)
	})

	if built {
		t.Run("a filter that widens the probe", func(t *testing.T) {
			// the filter allows widenDocs documents and vectorless objects
			// the index does not hold: more than 2 x kDocsToScore, so the
			// stages run, and fewer documents than kDocsToScore, so every
			// scan reaches too few and the probe is doubled. A first probe
			// of an eighth of the centroids, nearest the query, does not
			// reach the allowed documents, which lie opposite the query.
			// After maxWidenings doublings the probe asks for more
			// centroids than exist, the scan has reached every allowed
			// document, and the result is exact.
			centroids := countCentroids(t, shd)
			t.Logf("%d centroids, a first probe of %d", centroids, centroids/8+1)
			narrow := uc
			narrow.KCentroids = uint32(centroids/8 + 1)
			narrow.KDocsToScore = widenKDocs
			require.Greater(t, widenDocs+vectorless, 2*widenKDocs)
			require.Less(t, int(narrow.KCentroids), centroids)
			require.Greater(t, int(narrow.KCentroids)<<maxWidenings, centroids)
			require.NoError(t, shd.UpdateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{target: narrow}))
			defer func() {
				require.NoError(t, shd.UpdateVectorIndexConfigs(ctx, map[string]schemaConfig.VectorIndexConfig{target: uc}))
			}()

			allowed := func(id uint64) bool { return live(id) && nByID[id] < widenDocs }
			for qi, query := range widenQueries {
				require.Equal(t, bruteForceTop(query, docsByID, allowed, k),
					search(t, shd, query, intFilter(filters.OperatorEqual, "g", 1)), "query %d", qi)
			}
		})
	}

	t.Run("a batch into the index", func(t *testing.T) {
		more := tachiomfreshTestDocs(rng, extra, dims)
		batch := make([]*storobj.Object, extra)
		for i := range batch {
			batch[i] = newObject(amount+i, more[i])
		}
		for _, err := range shd.PutObjectBatch(ctx, batch) {
			require.NoError(t, err)
		}
		record(t, batch)
		waitForQueue(t, shd)
		checkAll(t, shd)
	})

	t.Run("an update under the same uuid", func(t *testing.T) {
		changed := tachiomfreshTestDocs(rng, 10, dims)
		for i := range changed {
			obj := newObject(200+i, changed[i])
			obj.Object.ID = objs[200+i].ID()
			require.NoError(t, shd.PutObject(ctx, obj))
			objs[200+i] = obj
		}
		record(t, objs[200:210])
		waitForQueue(t, shd)
		checkAll(t, shd)
	})

	t.Run("a delete and a re-insert", func(t *testing.T) {
		for _, obj := range objs[210:220] {
			require.NoError(t, shd.DeleteObject(ctx, obj.ID(), time.Now()))
			deleted[uuidToDoc[obj.ID()]] = true
		}
		waitForQueue(t, shd)
		checkAll(t, shd)
		for i, obj := range objs[210:220] {
			again := newObject(210+i, obj.MultiVectors[target])
			again.Object.ID = obj.ID()
			require.NoError(t, shd.PutObject(ctx, again))
		}
		record(t, objs[210:220])
		waitForQueue(t, shd)
		checkAll(t, shd)
	})

	t.Run("new token ids under the same uuid and tokens", func(t *testing.T) {
		// the token ids decide where the index places the tokens, so an
		// update that changes only them is stored and indexed again under
		// a new doc id: through a put, with or without a changed property,
		// through a batch and through a merge
		put := func(t *testing.T, objs []*storobj.Object) {
			for _, obj := range objs {
				require.NoError(t, shd.PutObject(ctx, obj))
			}
		}
		batch := func(t *testing.T, objs []*storobj.Object) {
			for _, err := range shd.PutObjectBatch(ctx, objs) {
				require.NoError(t, err)
			}
		}
		merge := func(t *testing.T, objs []*storobj.Object) {
			for _, obj := range objs {
				require.NoError(t, shd.MergeObject(ctx, objects.MergeDocument{
					Class:      className,
					ID:         obj.ID(),
					TokenIds:   obj.Object.TokenIds,
					UpdateTime: time.Now().UnixMilli(),
				}))
			}
		}
		for _, tt := range []struct {
			name    string
			objs    []*storobj.Object
			changeG bool
			write   func(t *testing.T, objs []*storobj.Object)
		}{
			{name: "a put, properties unchanged", objs: objs[220:225], write: put},
			{name: "a put, a property changed", objs: objs[225:230], changeG: true, write: put},
			{name: "a batch", objs: objs[230:235], write: batch},
			{name: "a merge of the token ids alone", objs: objs[235:240], write: merge},
		} {
			t.Run(tt.name, func(t *testing.T) {
				before := make([]uint64, len(tt.objs))
				next := make([]*storobj.Object, len(tt.objs))
				for i, obj := range tt.objs {
					before[i] = uuidToDoc[obj.ID()]
					tokens := obj.MultiVectors[target]
					again := newObject(int(obj.Object.Properties.(map[string]interface{})["n"].(int64)), tokens)
					again.Object.ID = obj.ID()
					shifted := make([]uint32, len(tokens))
					for pos := range shifted {
						shifted[pos] = uint32((pos + 1) % 3)
					}
					again.Object.TokenIds = models.TokenIds{target: shifted}
					if tt.changeG {
						again.Object.Properties.(map[string]interface{})["g"] = int64(2)
					}
					next[i] = again
				}
				tt.write(t, next)
				copy(tt.objs, next)
				record(t, tt.objs)
				for i, obj := range tt.objs {
					stored, err := shd.ObjectByID(ctx, obj.ID(), nil, additional.Properties{})
					require.NoError(t, err)
					require.Equal(t, obj.Object.TokenIds, stored.Object.TokenIds, "the stored token ids")
					require.Equal(t, obj.MultiVectors[target], stored.MultiVectors[target], "the stored tokens")
					require.NotEqual(t, before[i], stored.DocID, "the doc id")
				}
				waitForQueue(t, shd)
				checkAll(t, shd)
			})
		}
	})

	t.Run("deletes", func(t *testing.T) {
		for _, obj := range objs[100:160] {
			require.NoError(t, shd.DeleteObject(ctx, obj.ID(), time.Now()))
			deleted[uuidToDoc[obj.ID()]] = true
		}
		waitForQueue(t, shd)
		checkAll(t, shd)
	})

	t.Run("a restart", func(t *testing.T) {
		shardName := shd.Name()
		require.NoError(t, shd.Shutdown(ctx))
		reopened, err := idx.initShard(ctx, shardName, class, nil, true, true)
		require.NoError(t, err)
		idx.shards.Store(shardName, reopened)
		shd = reopened
		waitForQueue(t, shd)
		checkAll(t, shd)
	})

	t.Run("a single-vector query is refused", func(t *testing.T) {
		_, _, err := shd.ObjectVectorSearch(ctx, []models.Vector{[]float32{1, 2, 3, 4, 5, 6, 7, 8}}, []string{target},
			0, k, nil, nil, nil, additional.Properties{}, nil, nil)
		require.ErrorContains(t, err, "is configured for multi-vectors and does not accept single vectors")
	})

	// the files of the index: its buckets, the centroid graph's commit
	// log and its queues, every path that names the index. Empty files are
	// left out, as a backup leaves them out: the commit log that
	// PrepareForBackup has just opened is one.
	indexFiles := func(t *testing.T, shard *Shard) []string {
		var files []string
		require.NoError(t, filepath.WalkDir(shard.path(), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.Contains(path, indexID) {
				return err
			}
			info, err := d.Info()
			if err != nil || info.Size() == 0 {
				return err
			}
			rel, err := filepath.Rel(idx.Config.RootPath, path)
			files = append(files, rel)
			return err
		}))
		return files
	}

	t.Run("the backup lists every file of the index", func(t *testing.T) {
		shard := loadedTachiomShard(t, ctx, shd)
		require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
		defer func() {
			require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		}()
		require.False(t, shard.hasLegacyVectorIndex())
		listed, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
		onDisk := indexFiles(t, shard)
		require.NotEmpty(t, onDisk)
		// the buckets that hold data: a staged index has written only its
		// staged documents and its type bucket (the phase); a built one has
		// emptied the staging bucket
		withData := []string{
			helpers.TachiomFreshTypesBucketName(indexID),
			helpers.TachiomFreshStagingBucketName(indexID),
		}
		if built {
			withData = []string{
				helpers.TachiomFreshPostingsBucketName(indexID),
				helpers.TachiomFreshTypesBucketName(indexID),
				helpers.TachiomFreshTokenCountsBucketName(indexID),
				helpers.HFreshSharedBucketName(indexID),
			}
		}
		for _, bucket := range withData {
			var found bool
			for _, file := range onDisk {
				found = found || strings.Contains(file, bucket)
			}
			require.True(t, found, "no file of bucket %s on disk", bucket)
		}
		for _, file := range onDisk {
			require.Contains(t, listed, file)
		}
	})

	t.Run("dropping the vector index leaves no file of it", func(t *testing.T) {
		shard := loadedTachiomShard(t, ctx, shd)
		paths := func() []string {
			var found []string
			require.NoError(t, filepath.WalkDir(shard.path(), func(path string, _ fs.DirEntry, err error) error {
				if err == nil && strings.Contains(path, indexID) {
					found = append(found, path)
				}
				return err
			}))
			return found
		}
		require.NotEmpty(t, paths())
		class.VectorConfig[target] = models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}
		require.NoError(t, shard.DropVectorIndex(ctx, target))
		require.Empty(t, paths(), "files or directories of the dropped index")
	})
}

// loadedTachiomShard returns the shard behind a ShardLike, loading a lazy one.
func loadedTachiomShard(t *testing.T, ctx context.Context, shd ShardLike) *Shard {
	t.Helper()
	switch s := shd.(type) {
	case *Shard:
		return s
	case *LazyLoadShard:
		shard, _, err := s.loadIfCold(ctx)
		require.NoError(t, err)
		return shard
	default:
		t.Fatalf("unexpected shard type %T", shd)
		return nil
	}
}

// TestShard_TachiomFreshThunksAgree: the three readers the shard hands the
// index (the whole document, the document into a container, one token by
// position) return the same floats for the same object, on a real shard,
// in the memtable and after a flush.
func TestShard_TachiomFreshThunksAgree(t *testing.T) {
	ctx := context.Background()
	const (
		className = "TachiomFreshThunks"
		target    = "colbert"
		dims      = 6
		amount    = 20
	)
	rng := rand.New(rand.NewSource(7))
	docs := tachiomfreshTestDocs(rng, amount, dims)

	uc := tachiomfreshent.NewDefaultUserConfig()
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, nil, false, false,
		func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{target: uc}
		})
	defer func() {
		require.NoError(t, idx.drop())
	}()
	shard := shd.(*Shard)

	objs := make([]*storobj.Object, amount)
	for i := range objs {
		objs[i] = testObject(className)
		objs[i].MultiVectors = map[string][][]float32{target: docs[i]}
		objs[i].Object.TokenIds = models.TokenIds{target: tachiomfreshTestTokenIDs(len(docs[i]))}
	}
	for _, err := range shard.PutObjectBatch(ctx, objs) {
		require.NoError(t, err)
	}
	docIDs := docIDsOf(t, ctx, shard, objs)

	check := func(t *testing.T) {
		container := &common.VectorSlice{Buff8: make([]byte, 8)}
		for i, want := range docs {
			id := docIDs[i]
			whole, err := shard.multiVectorByIndexID(ctx, id, target)
			require.NoError(t, err)
			require.Equal(t, want, whole, "doc %d, whole document", id)

			buffered, err := shard.readMultiVectorByIndexIDIntoSlice(ctx, id, container, target)
			require.NoError(t, err)
			require.Equal(t, want, buffered, "doc %d, buffered document", id)

			for pos := range want {
				token, err := shard.tokenByIndexID(ctx, id, pos, target, dims)
				require.NoError(t, err)
				require.Equal(t, want[pos], token, "doc %d token %d", id, pos)
			}

			// an object without the asked multi-vector: all three readers
			// report it as not found, as for a single vector
			const missing = "missing"
			_, wholeErr := shard.multiVectorByIndexID(ctx, id, missing)
			_, bufferedErr := shard.readMultiVectorByIndexIDIntoSlice(ctx, id, container, missing)
			_, tokenErr := shard.tokenByIndexID(ctx, id, 0, missing, dims)
			for _, err := range []error{wholeErr, bufferedErr, tokenErr} {
				var notFound storobj.ErrNotFound
				require.True(t, stderrors.As(err, &notFound), "doc %d: %v", id, err)
			}
		}
	}

	tests := []struct {
		name  string
		setup func(t *testing.T)
	}{
		{name: "in the memtable", setup: func(*testing.T) {}},
		{name: "in a segment", setup: func(t *testing.T) {
			bucket, release, err := shard.objectsBucket()
			require.NoError(t, err)
			defer release()
			require.NoError(t, bucket.FlushMemtable())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(t)
			check(t)
		})
	}
}
