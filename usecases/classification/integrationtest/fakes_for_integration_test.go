//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package classification_integration_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

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

func singleShardState() *sharding.State {
	config, err := sharding.ParseConfig(nil, 1)
	if err != nil {
		panic(err)
	}

	s, err := sharding.InitState("test-index", config,
		fakeNodes{[]string{"node1"}})
	if err != nil {
		panic(err)
	}

	return s
}

type fakeClassificationRepo struct {
	sync.Mutex
	db map[strfmt.UUID]models.Classification
}

func newFakeClassificationRepo() *fakeClassificationRepo {
	return &fakeClassificationRepo{
		db: map[strfmt.UUID]models.Classification{},
	}
}

func (f *fakeClassificationRepo) Put(ctx context.Context, class models.Classification) error {
	f.Lock()
	defer f.Unlock()

	f.db[class.ID] = class
	return nil
}

func (f *fakeClassificationRepo) Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error) {
	f.Lock()
	defer f.Unlock()

	class, ok := f.db[id]
	if !ok {
		return nil, nil
	}

	return &class, nil
}

func testSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "ExactCategory",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
				},
				{
					Class:               "MainCategory",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
				},
				{
					Class:               "Article",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "description",
							DataType: []string{string(schema.DataTypeText)},
						},
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "exactCategory",
							DataType: []string{"ExactCategory"},
						},
						{
							Name:     "mainCategory",
							DataType: []string{"MainCategory"},
						},
						{
							Name:     "categories",
							DataType: []string{"ExactCategory"},
						},
						{
							Name:     "anyCategory",
							DataType: []string{"MainCategory", "ExactCategory"},
						},
					},
				},
			},
		},
	}
}

// only used for knn-type
func testDataAlreadyClassified() search.Results {
	return search.Results{
		search.Result{
			ID:        "8aeecd06-55a0-462c-9853-81b31a284d80",
			ClassName: "Article",
			Vector:    []float32{1, 0, 0},
			Schema: map[string]interface{}{
				"description":   "This article talks about politics",
				"exactCategory": models.MultipleRef{beaconRef(idCategoryPolitics)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryPoliticsAndSociety)},
			},
		},
		search.Result{
			ID:        "9f4c1847-2567-4de7-8861-34cf47a071ae",
			ClassName: "Article",
			Vector:    []float32{0, 1, 0},
			Schema: map[string]interface{}{
				"description":   "This articles talks about society",
				"exactCategory": models.MultipleRef{beaconRef(idCategorySociety)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryPoliticsAndSociety)},
			},
		},
		search.Result{
			ID:        "926416ec-8fb1-4e40-ab8c-37b226b3d68e",
			ClassName: "Article",
			Vector:    []float32{0, 0, 1},
			Schema: map[string]interface{}{
				"description":   "This article talks about food",
				"exactCategory": models.MultipleRef{beaconRef(idCategoryFoodAndDrink)},
				"mainCategory":  models.MultipleRef{beaconRef(idMainCategoryFoodAndDrink)},
			},
		},
	}
}

// only used for zeroshot-type
func testDataZeroShotUnclassified() search.Results {
	return search.Results{
		search.Result{
			ID:        "8aeecd06-55a0-462c-9853-81b31a284d80",
			ClassName: "FoodType",
			Vector:    []float32{1, 0, 0},
			Schema: map[string]interface{}{
				"text": "Ice cream",
			},
		},
		search.Result{
			ID:        "9f4c1847-2567-4de7-8861-34cf47a071ae",
			ClassName: "FoodType",
			Vector:    []float32{0, 1, 0},
			Schema: map[string]interface{}{
				"text": "Meat",
			},
		},
		search.Result{
			ID:        "926416ec-8fb1-4e40-ab8c-37b226b3d68e",
			ClassName: "Recipes",
			Vector:    []float32{0, 0, 1},
			Schema: map[string]interface{}{
				"text": "Cut the steak in half and put it into pan",
			},
		},
		search.Result{
			ID:        "926416ec-8fb1-4e40-ab8c-37b226b3d688",
			ClassName: "Recipes",
			Vector:    []float32{0, 1, 1},
			Schema: map[string]interface{}{
				"description": "There are flavors of vanilla, chocolate and strawberry",
			},
		},
	}
}

func mustUUID() strfmt.UUID {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}

	return strfmt.UUID(id.String())
}

func largeTestDataSize(size int) search.Results {
	out := make(search.Results, size)

	for i := range out {
		out[i] = search.Result{
			ID:        mustUUID(),
			ClassName: "Article",
			Vector:    []float32{0.02, 0, rand.Float32()},
			Schema: map[string]interface{}{
				"description": "does not matter much",
			},
		}
	}
	return out
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

func beaconRef(target string) *models.SingleRef {
	beacon := fmt.Sprintf("weaviate://localhost/%s", target)
	return &models.SingleRef{Beacon: strfmt.URI(beacon)}
}

const (
	idMainCategoryPoliticsAndSociety = "39c6abe3-4bbe-4c4e-9e60-ca5e99ec6b4e"
	idMainCategoryFoodAndDrink       = "5a3d909a-4f0d-4168-8f5c-cd3074d1e79a"
	idCategoryPolitics               = "1b204f16-7da6-44fd-bbd2-8cc4a7414bc3"
	idCategorySociety                = "ec500f39-1dc9-4580-9bd1-55a8ea8e37a2"
	idCategoryFoodAndDrink           = "027b708a-31ca-43ea-9001-88bec864c79c"
)

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
	}
}

func testSchemaForZeroShot() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "FoodType",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: []string{string(schema.DataTypeText)},
						},
					},
				},
				{
					Class:               "Recipes",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: []string{string(schema.DataTypeText)},
						},
						{
							Name:     "ofFoodType",
							DataType: []string{"FoodType"},
						},
					},
				},
			},
		},
	}
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

type fakeRemoteClient struct{}

func (f *fakeRemoteClient) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object,
) error {
	return nil
}

func (f *fakeRemoteClient) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	return false, nil
}

func (f *fakeRemoteClient) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) error {
	return nil
}

func (f *fakeRemoteClient) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument,
) error {
	return nil
}

func (f *fakeRemoteClient) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	return nil, nil, nil
}

func (f *fakeRemoteClient) BatchPutObjects(ctx context.Context, hostName, indexName,
	shardName string, obj []*storobj.Object,
) []error {
	return nil
}

func (f *fakeRemoteClient) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) BatchAddReferences(ctx context.Context, hostName,
	indexName, shardName string, refs objects.BatchReferences,
) []error {
	return nil
}

func (f *fakeRemoteClient) Aggregate(ctx context.Context, hostName, indexName,
	shardName string, params aggregation.Params,
) (*aggregation.Result, error) {
	return nil, nil
}

func (f *fakeRemoteClient) FindDocIDs(ctx context.Context, hostName, indexName, shardName string,
	filters *filters.LocalFilter,
) ([]uint64, error) {
	return nil, nil
}

func (f *fakeRemoteClient) DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
	docIDs []uint64, dryRun bool,
) objects.BatchSimpleObjects {
	return nil
}

func (f *fakeRemoteClient) GetShardStatus(ctx context.Context,
	hostName, indexName, shardName string,
) (string, error) {
	return "", nil
}

func (f *fakeRemoteClient) UpdateShardStatus(ctx context.Context, hostName, indexName, shardName,
	targetStatus string,
) error {
	return nil
}

type fakeNodeResolver struct{}

func (f *fakeNodeResolver) NodeHostname(string) (string, bool) {
	return "", false
}
