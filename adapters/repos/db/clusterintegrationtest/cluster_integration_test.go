//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package clusterintegrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/clients"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const vectorDims = 20

// TestDistributedSetup uses as many real components and only mocks out
// non-essential parts. Essentially we fix the shard/cluster state and schema
// as they aren't critical to this test, but use real repos and real HTTP APIs
// between the repos.
func TestDistributedSetup(t *testing.T) {
	t.Run("individual imports", func(t *testing.T) {
		dirName, cleanup := setupDirectory()
		defer cleanup()

		testDistributed(t, dirName, false)
	})

	t.Run("batched imports", func(t *testing.T) {
		dirName, cleanup := setupDirectory()
		defer cleanup()

		testDistributed(t, dirName, true)
	})
}

func testDistributed(t *testing.T, dirName string, batch bool) {
	var nodes []*node
	numberOfNodes := 10
	numberOfObjects := 200

	t.Run("setup", func(t *testing.T) {
		overallShardState := multiShardState(numberOfNodes)
		shardStateSerialized, err := json.Marshal(overallShardState)
		require.Nil(t, err)

		for i := 0; i < numberOfNodes; i++ {
			node := &node{
				name: fmt.Sprintf("node-%d", i),
			}

			node.init(numberOfNodes, dirName, shardStateSerialized, &nodes)
			nodes = append(nodes, node)
		}
	})

	t.Run("apply schema", func(t *testing.T) {
		for i := range nodes {
			err := nodes[i].migrator.AddClass(context.Background(), class(),
				nodes[i].schemaGetter.shardState)
			require.Nil(t, err)
			err = nodes[i].migrator.AddClass(context.Background(), secondClassWithRef(),
				nodes[i].schemaGetter.shardState)
			require.Nil(t, err)
			nodes[i].schemaGetter.schema.Objects.Classes = append(nodes[i].schemaGetter.schema.Objects.Classes,
				class(), secondClassWithRef())
		}
	})

	data := exampleData(numberOfObjects)
	refData := exampleDataWithRefs(numberOfObjects, 5, data)

	if batch {
		t.Run("import large batch from random node", func(t *testing.T) {
			// pick a random node, but send the entire batch to this node
			node := nodes[rand.Intn(len(nodes))]

			batchObjs := dataAsBatch(data)
			res, err := node.repo.BatchPutObjects(context.Background(), batchObjs)
			require.Nil(t, err)
			for _, ind := range res {
				require.Nil(t, ind.Err)
			}
		})

		t.Run("import second class without refs", func(t *testing.T) {
			// pick a random node, but send the entire batch to this node
			node := nodes[rand.Intn(len(nodes))]

			batchObjs := dataAsBatchWithProps(refData, []string{"description"})
			res, err := node.repo.BatchPutObjects(context.Background(), batchObjs)
			require.Nil(t, err)
			for _, ind := range res {
				require.Nil(t, ind.Err)
			}
		})

		t.Run("import refs as batch", func(t *testing.T) {
			// pick a random node, but send the entire batch to this node
			node := nodes[rand.Intn(len(nodes))]

			batch := refsAsBatch(refData, "toFirst")
			res, err := node.repo.AddBatchReferences(context.Background(), batch)
			require.Nil(t, err)
			for _, ind := range res {
				require.Nil(t, ind.Err)
			}
		})
	} else {
		t.Run("import first class by picking a random node", func(t *testing.T) {
			for _, obj := range data {
				node := nodes[rand.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector)
				require.Nil(t, err)
			}
		})

		t.Run("import second class with refs by picking a random node", func(t *testing.T) {
			for _, obj := range refData {
				node := nodes[rand.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector)
				require.Nil(t, err)
			}
		})
	}

	t.Run("query individually to check if all exist using random nodes", func(t *testing.T) {
		for _, obj := range data {
			node := nodes[rand.Intn(len(nodes))]

			ok, err := node.repo.Exists(context.Background(), obj.ID)
			require.Nil(t, err)
			assert.True(t, ok)
		}
	})

	t.Run("query individually using random node", func(t *testing.T) {
		for _, obj := range data {
			node := nodes[rand.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID,
				search.SelectProperties{}, additional.Properties{})
			require.Nil(t, err)
			require.NotNil(t, res)

			// only compare string prop to avoid having to deal with parsing time
			// props
			assert.Equal(t, obj.Properties.(map[string]interface{})["description"],
				res.Object().Properties.(map[string]interface{})["description"])
		}
	})

	t.Run("perform vector searches", func(t *testing.T) {
		// note this test assumes a recall of 100% which only works with HNSW on
		// small sizes, so if we use this test suite with massive sizes, we should
		// not expect this test to succeed 100% of times anymore.
		runs := 10

		for i := 0; i < runs; i++ {
			query := make([]float32, vectorDims)
			for i := range query {
				query[i] = rand.Float32()
			}

			groundTruth := bruteForceObjectsByQuery(data, query)

			node := nodes[rand.Intn(len(nodes))]
			res, err := node.repo.VectorClassSearch(context.Background(), traverser.GetParams{
				SearchVector: query,
				Pagination: &filters.Pagination{
					Limit: 25,
				},
				ClassName: "Distributed",
			})
			assert.Nil(t, err)
			for i, obj := range res {
				assert.Equal(t, groundTruth[i].ID, obj.ID, fmt.Sprintf("at pos %d", i))
			}
		}

		for _, obj := range data {
			node := nodes[rand.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID, search.SelectProperties{}, additional.Properties{})
			require.Nil(t, err)
			require.NotNil(t, res)

			// only compare string prop to avoid having to deal with parsing time
			// props
			assert.Equal(t, obj.Properties.(map[string]interface{})["description"],
				res.Object().Properties.(map[string]interface{})["description"])
		}
	})

	t.Run("query individually and resolve references", func(t *testing.T) {
		for _, obj := range refData {
			// if i == 5 {
			// 	break
			// }
			node := nodes[rand.Intn(len(nodes))]

			res, err := node.repo.ObjectByID(context.Background(), obj.ID,
				search.SelectProperties{
					search.SelectProperty{
						Name:        "toFirst",
						IsPrimitive: false,
						Refs: []search.SelectClass{
							search.SelectClass{
								ClassName: "Distributed",
								RefProperties: search.SelectProperties{
									search.SelectProperty{
										Name:        "description",
										IsPrimitive: true,
									},
								},
							},
						},
					},
				}, additional.Properties{})
			require.Nil(t, err)
			require.NotNil(t, res)
			props := res.Object().Properties.(map[string]interface{})
			refProp, ok := props["toFirst"].([]interface{})
			require.True(t, ok)

			var refPayload []map[string]interface{}
			for _, res := range refProp {
				parsed, ok := res.(search.LocalRef)
				require.True(t, ok)
				refPayload = append(refPayload, map[string]interface{}{
					"description": parsed.Fields["description"],
				})
			}

			actual := manuallyResolveRef(t, obj, data, "toFirst", "description")
			assert.Equal(t, actual, refPayload)
		}
	})

	t.Run("aggreate count", func(t *testing.T) {
		params := aggregation.Params{
			ClassName:        schema.ClassName("Distributed"),
			IncludeMetaCount: true,
		}

		node := nodes[rand.Intn(len(nodes))]
		res, err := node.repo.Aggregate(context.Background(), params)
		require.Nil(t, err)

		expectedResult := &aggregation.Result{
			Groups: []aggregation.Group{
				aggregation.Group{
					Count: numberOfObjects,
				},
			},
		}

		assert.Equal(t, expectedResult, res)
	})

	t.Run("modify an object using patch", func(t *testing.T) {
		obj := data[0]

		node := nodes[rand.Intn(len(nodes))]
		err := node.repo.Merge(context.Background(), objects.MergeDocument{
			Class: "Distributed",
			ID:    obj.ID,
			PrimitiveSchema: map[string]interface{}{
				"other_property": "a-value-inserted-through-merge",
			},
		})

		require.Nil(t, err)
	})

	t.Run("verify the patched object contains the additions and orig", func(t *testing.T) {
		obj := data[0]

		node := nodes[rand.Intn(len(nodes))]
		res, err := node.repo.ObjectByID(context.Background(), obj.ID,
			search.SelectProperties{}, additional.Properties{})

		require.Nil(t, err)
		previousMap := obj.Properties.(map[string]interface{})
		assert.Equal(t, map[string]interface{}{
			"other_property": "a-value-inserted-through-merge",
			"description":    previousMap["description"],
			"date_property":  previousMap["date_property"].(time.Time).Format(time.RFC3339),
		}, res.Object().Properties)
	})

	// This test prevents a regression on
	// https://github.com/semi-technologies/weaviate/issues/1775
	t.Run("query items by date filter", func(t *testing.T) {
		count := len(data) / 2 // try to match half the data objects present
		cutoff := time.Unix(0, 0).Add(time.Duration(count) * time.Hour)
		node := nodes[rand.Intn(len(nodes))]
		res, err := node.repo.ClassSearch(context.Background(), traverser.GetParams{
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorLessThan,
					On: &filters.Path{
						Class:    "Distributed",
						Property: schema.PropertyName("date_property"),
					},
					Value: &filters.Value{
						Value: cutoff,
						Type:  schema.DataTypeDate,
					},
				},
			},
			ClassName: "Distributed",
			Pagination: &filters.Pagination{
				Limit: len(data),
			},
		})

		require.Nil(t, err)
		assert.Equal(t, count, len(res))
	})

	t.Run("delete a third of the data from random nodes", func(t *testing.T) {
		for i, obj := range data {
			if i%3 != 0 {
				// keep this item
				continue
			}

			node := nodes[rand.Intn(len(nodes))]
			err := node.repo.DeleteObject(context.Background(), "Distributed", obj.ID)
			require.Nil(t, err)
		}
	})

	t.Run("make sure 2/3 exist, 1/3 no longer exists", func(t *testing.T) {
		for i, obj := range data {
			expected := true
			if i%3 == 0 {
				expected = false
			}

			node := nodes[rand.Intn(len(nodes))]
			actual, err := node.repo.Exists(context.Background(), obj.ID)
			require.Nil(t, err)
			assert.Equal(t, expected, actual)
		}
	})
}

func setupDirectory() (string, func()) {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	return dirName, func() {
		err := os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func dataAsBatch(data []*models.Object) objects.BatchObjects {
	batchObjs := make(objects.BatchObjects, len(data))
	for i := range data {
		batchObjs[i] = objects.BatchObject{
			OriginalIndex: i,
			Err:           nil,
			Object:        data[i],
			UUID:          data[i].ID,
			Vector:        data[i].Vector,
		}
	}

	return batchObjs
}

func dataAsBatchWithProps(data []*models.Object, props []string) objects.BatchObjects {
	batchObjs := make(objects.BatchObjects, len(data))
	for i := range data {
		batchObjs[i] = objects.BatchObject{
			OriginalIndex: i,
			Err:           nil,
			Object:        copyObjectWithProp(data[i], props),
			UUID:          data[i].ID,
			Vector:        data[i].Vector,
		}
	}

	return batchObjs
}

// copyObjectWithProp is not a 100% copy. It may still contain the same
// pointers in some properties, it does however guarantee that it does not
// alter the existing input - this guarantee is lost, if you modify the output
func copyObjectWithProp(in *models.Object, propsToCopy []string) *models.Object {
	out := &models.Object{}

	out.Additional = in.Additional
	out.Class = in.Class
	out.Vector = in.Vector
	out.CreationTimeUnix = in.CreationTimeUnix
	out.LastUpdateTimeUnix = in.LastUpdateTimeUnix
	out.ID = in.ID
	props := map[string]interface{}{}

	for _, propName := range propsToCopy {
		props[propName] = in.Properties.(map[string]interface{})[propName]
	}

	out.Properties = props
	return out
}

type node struct {
	name             string
	shardingState    *sharding.State
	repo             *db.DB
	schemaGetter     *fakeSchemaGetter
	clusterAPIServer *httptest.Server
	migrator         *db.Migrator
	hostname         string
}

func (n *node) init(numberOfNodes int, dirName string, shardStateRaw []byte,
	allNodes *[]*node) {
	localDir := path.Join(dirName, n.name)
	logger, _ := test.NewNullLogger()

	nodeResolver := &nodeResolver{
		nodes: allNodes,
		local: n.name,
	}

	shardState, err := sharding.StateFromJSON(shardStateRaw, nodeResolver)
	if err != nil {
		panic(err)
	}

	client := clients.NewRemoteIndex(&http.Client{})
	n.repo = db.New(logger, db.Config{RootPath: localDir, QueryMaximumResults: 10000}, client, nodeResolver)
	n.schemaGetter = &fakeSchemaGetter{
		shardState: shardState,
		schema:     schema.Schema{Objects: &models.Schema{}},
	}
	n.repo.SetSchemaGetter(n.schemaGetter)
	err = n.repo.WaitForStartup(context.Background())
	if err != nil {
		panic(err)
	}

	n.migrator = db.NewMigrator(n.repo, logger)

	indices := clusterapi.NewIndices(sharding.NewRemoteIndexIncoming(n.repo))
	mux := http.NewServeMux()
	mux.Handle("/indices/", indices.Indices())

	srv := httptest.NewServer(mux)
	u, err := url.Parse(srv.URL)
	if err != nil {
		panic(err)
	}
	n.hostname = u.Host
}

func multiShardState(nodeCount int) *sharding.State {
	config, err := sharding.ParseConfig(map[string]interface{}{
		"desiredCount": json.Number(fmt.Sprintf("%d", nodeCount)),
	}, 1)
	if err != nil {
		panic(err)
	}

	nodeList := make([]string, nodeCount)
	for i := range nodeList {
		nodeList[i] = fmt.Sprintf("node-%d", i)
	}

	s, err := sharding.InitState("multi-shard-test-index", config,
		fakeNodes{nodeList})
	if err != nil {
		panic(err)
	}

	return s
}

type fakeNodes struct {
	nodes []string
}

func (f fakeNodes) AllNames() []string {
	return f.nodes
}

func (f fakeNodes) LocalName() string {
	return f.nodes[0]
}

type fakeSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) ShardingState(class string) *sharding.State {
	return f.shardState
}

type nodeResolver struct {
	nodes *[]*node
	local string
}

func (r nodeResolver) AllNames() []string {
	panic("node resolving not implemented yet")
}

func (r nodeResolver) LocalName() string {
	return r.local
}

func (r nodeResolver) NodeHostname(nodeName string) (string, bool) {
	for _, node := range *r.nodes {
		if node.name == nodeName {
			return node.hostname, true
		}
	}

	return "", false
}

func class() *models.Class {
	cfg := hnsw.NewDefaultUserConfig()
	cfg.EF = 500
	return &models.Class{
		Class:               "Distributed",
		VectorIndexConfig:   cfg,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "other_property",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "date_property",
				DataType: []string{string(schema.DataTypeDate)},
			},
		},
	}
}

func secondClassWithRef() *models.Class {
	cfg := hnsw.NewDefaultUserConfig()
	cfg.EF = 500
	return &models.Class{
		Class:               "SecondDistributed",
		VectorIndexConfig:   cfg,
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "toFirst",
				DataType: []string{"Distributed"},
			},
		},
	}
}

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
	}
}

func exampleData(size int) []*models.Object {
	out := make([]*models.Object, size)

	for i := range out {
		vec := make([]float32, vectorDims)
		for i := range vec {
			vec[i] = rand.Float32()
		}

		timestamp := time.Unix(0, 0).Add(time.Duration(i) * time.Hour)

		out[i] = &models.Object{
			Class: "Distributed",
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"description":   fmt.Sprintf("object-%d", i),
				"date_property": timestamp,
			},
			Vector: vec,
		}
	}

	return out
}

func exampleDataWithRefs(size int, refCount int, targetObjs []*models.Object) []*models.Object {
	out := make([]*models.Object, size)

	for i := range out {
		vec := make([]float32, vectorDims)
		for i := range vec {
			vec[i] = rand.Float32()
		}

		refs := make(models.MultipleRef, refCount)
		for i := range refs {
			randomTarget := targetObjs[rand.Intn(len(targetObjs))]
			refs[i] = crossref.New("localhost", randomTarget.ID).SingleRef()
		}

		out[i] = &models.Object{
			Class: "SecondDistributed",
			ID:    strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{
				"description": fmt.Sprintf("second-object-%d", i),
				"toFirst":     refs,
			},
			Vector: vec,
		}
	}

	return out
}

func bruteForceObjectsByQuery(objs []*models.Object,
	query []float32) []*models.Object {
	type distanceAndObj struct {
		distance float32
		obj      *models.Object
	}

	distProv := distancer.NewDotProductProvider()
	distances := make([]distanceAndObj, len(objs))

	for i := range objs {
		dist, _, _ := distProv.SingleDist(normalize(query), normalize(objs[i].Vector))
		distances[i] = distanceAndObj{
			distance: dist,
			obj:      objs[i],
		}
	}

	sort.Slice(distances, func(a, b int) bool {
		return distances[a].distance < distances[b].distance
	})

	out := make([]*models.Object, len(objs))
	for i := range out {
		out[i] = distances[i].obj
	}

	return out
}

func normalize(v []float32) []float32 {
	var norm float32
	for i := range v {
		norm += v[i] * v[i]
	}

	norm = float32(math.Sqrt(float64(norm)))
	for i := range v {
		v[i] = v[i] / norm
	}

	return v
}

func manuallyResolveRef(t *testing.T, obj *models.Object,
	possibleTargets []*models.Object, localPropName,
	referencedPropName string) []map[string]interface{} {
	beacons := obj.Properties.(map[string]interface{})[localPropName].(models.MultipleRef)
	out := make([]map[string]interface{}, len(beacons))

	for i, ref := range beacons {
		parsed, err := crossref.Parse(ref.Beacon.String())
		require.Nil(t, err)
		target := findId(possibleTargets, parsed.TargetID)
		require.NotNil(t, target, "target not found")
		out[i] = map[string]interface{}{
			referencedPropName: target.Properties.(map[string]interface{})[referencedPropName],
		}
	}

	return out
}

func findId(list []*models.Object, id strfmt.UUID) *models.Object {
	for _, obj := range list {
		if obj.ID == id {
			return obj
		}
	}

	return nil
}

func refsAsBatch(in []*models.Object, propName string) objects.BatchReferences {
	out := objects.BatchReferences{}

	originalIndex := 0
	for _, obj := range in {
		beacons := obj.Properties.(map[string]interface{})[propName].(models.MultipleRef)
		current := make(objects.BatchReferences, len(beacons))
		for i, beacon := range beacons {
			to, err := crossref.Parse(beacon.Beacon.String())
			if err != nil {
				panic(err)
			}
			current[i] = objects.BatchReference{
				OriginalIndex: originalIndex,
				To:            to,
				From: crossref.NewSource(schema.ClassName(obj.Class),
					schema.PropertyName(propName), obj.ID),
			}
			originalIndex++
		}
		out = append(out, current...)
	}

	return out
}
