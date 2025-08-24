//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package classification_integration_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/dto"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

type fakeSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) ReadOnlyClass(className string) *models.Class {
	return f.schema.GetClass(className)
}

func (f *fakeSchemaGetter) CopyShardingState(class string) *sharding.State {
	return f.shardState
}

func (f *fakeSchemaGetter) ShardOwner(class, shard string) (string, error) {
	ss := f.shardState
	x, ok := ss.Physical[shard]
	if !ok {
		return "", fmt.Errorf("shard not found")
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return ss.Physical[shard].BelongsToNodes[0], nil
}

func (f *fakeSchemaGetter) ShardReplicas(class, shard string) ([]string, error) {
	ss := f.shardState
	x, ok := ss.Physical[shard]
	if !ok {
		return nil, fmt.Errorf("shard not found")
	}
	return x.BelongsToNodes, nil
}

func (f *fakeSchemaGetter) TenantsShards(_ context.Context, class string, tenants ...string) (map[string]string, error) {
	res := map[string]string{}
	for _, t := range tenants {
		res[t] = models.TenantActivityStatusHOT
	}
	return res, nil
}

func (f *fakeSchemaGetter) OptimisticTenantStatus(_ context.Context, class string, tenant string) (map[string]string, error) {
	res := map[string]string{}
	res[tenant] = models.TenantActivityStatusHOT
	return res, nil
}

func (f *fakeSchemaGetter) ShardFromUUID(class string, uuid []byte) string {
	ss := f.shardState
	return ss.Shard("", string(uuid))
}

func (f *fakeSchemaGetter) Nodes() []string {
	return []string{"node1"}
}

func (m *fakeSchemaGetter) NodeName() string {
	return "node1"
}

func (m *fakeSchemaGetter) ClusterHealthScore() int {
	return 0
}

func (m *fakeSchemaGetter) Statistics() map[string]any {
	return nil
}

func (m *fakeSchemaGetter) ResolveParentNodes(_ string, shard string,
) (map[string]string, error) {
	return nil, nil
}

func singleShardState() *sharding.State {
	config, err := shardingConfig.ParseConfig(nil, 1)
	if err != nil {
		panic(err)
	}

	selector := mocks.NewMockNodeSelector("node1")
	s, err := sharding.InitState("test-index", config, selector.LocalName(),
		selector.StorageCandidates(), 1, false)
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
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
		UsingBlockMaxWAND:      config.DefaultUsingBlockMaxWAND,
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

type fakeRemoteClient struct{}

func (f *fakeRemoteClient) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) PutFile(ctx context.Context, hostName, indexName,
	shardName, fileName string, payload io.ReadSeekCloser,
) error {
	return nil
}

func (f *fakeRemoteClient) GetFileMetadata(ctx context.Context, hostName, indexName, shardName, fileName string) (file.FileMetadata, error) {
	return file.FileMetadata{}, nil
}

func (f *fakeRemoteClient) GetFile(ctx context.Context, hostName, indexName, shardName, fileName string) (io.ReadCloser, error) {
	return nil, nil
}

func (f *fakeRemoteClient) PauseFileActivity(ctx context.Context,
	hostName, indexName, shardName string, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) ResumeFileActivity(ctx context.Context,
	hostName, indexName, shardName string,
) error {
	return nil
}

func (f *fakeRemoteClient) ListFiles(ctx context.Context,
	hostName, indexName, shardName string,
) ([]string, error) {
	return nil, nil
}

func (f *fakeRemoteClient) AddAsyncReplicationTargetNode(ctx context.Context, hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error {
	return nil
}

func (f *fakeRemoteClient) RemoveAsyncReplicationTargetNode(ctx context.Context, hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	return nil
}

func (f *fakeRemoteClient) GetObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) FindObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties,
) (*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) OverwriteObjects(ctx context.Context,
	host, index, shard string, objects []*objects.VObject,
) ([]types.RepairResponse, error) {
	return nil, nil
}

func (f *fakeRemoteClient) Exists(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID,
) (bool, error) {
	return false, nil
}

func (f *fakeRemoteClient) DeleteObject(ctx context.Context, hostName, indexName,
	shardName string, id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []models.Vector, targetVector []string, distance float32, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	return nil, nil, nil
}

func (f *fakeRemoteClient) BatchPutObjects(ctx context.Context, hostName, indexName, shardName string, objs []*storobj.Object, repl *additional.ReplicationProperties, schemaVersion uint64) []error {
	return nil
}

func (f *fakeRemoteClient) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) BatchAddReferences(ctx context.Context, hostName,
	indexName, shardName string, refs objects.BatchReferences, schemaVersion uint64,
) []error {
	return nil
}

func (f *fakeRemoteClient) Aggregate(ctx context.Context, hostName, indexName,
	shardName string, params aggregation.Params,
) (*aggregation.Result, error) {
	return nil, nil
}

func (f *fakeRemoteClient) FindUUIDs(ctx context.Context, hostName, indexName, shardName string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	return nil, nil
}

func (f *fakeRemoteClient) DeleteObjectBatch(ctx context.Context, hostName, indexName, shardName string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) objects.BatchSimpleObjects {
	return nil
}

func (f *fakeRemoteClient) GetShardQueueSize(ctx context.Context,
	hostName, indexName, shardName string,
) (int64, error) {
	return 0, nil
}

func (f *fakeRemoteClient) GetShardStatus(ctx context.Context,
	hostName, indexName, shardName string,
) (string, error) {
	return "", nil
}

func (f *fakeRemoteClient) UpdateShardStatus(ctx context.Context, hostName, indexName, shardName,
	targetStatus string, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) DigestObjects(ctx context.Context,
	hostName, indexName, shardName string, ids []strfmt.UUID,
) (result []types.RepairResponse, err error) {
	return nil, nil
}

type fakeNodeResolver struct{}

func (f *fakeNodeResolver) AllHostnames() []string {
	return nil
}

func (f *fakeNodeResolver) NodeHostname(string) (string, bool) {
	return "", false
}

type fakeRemoteNodeClient struct{}

func (f *fakeRemoteNodeClient) GetNodeStatus(ctx context.Context, hostName, className, shardName, output string) (*models.NodeStatus, error) {
	return &models.NodeStatus{}, nil
}

func (f *fakeRemoteNodeClient) GetStatistics(ctx context.Context, hostName string) (*models.Statistics, error) {
	return &models.Statistics{}, nil
}

type fakeReplicationClient struct{}

var _ replica.Client = (*fakeReplicationClient)(nil)

func (f *fakeReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	mergeDoc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (f *fakeReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	return nil
}

func (f *fakeReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	return replica.SimpleResponse{}, nil
}

func (c *fakeReplicationClient) Exists(ctx context.Context, host, index,
	shard string, id strfmt.UUID,
) (bool, error) {
	return false, nil
}

func (f *fakeReplicationClient) FetchObject(_ context.Context, host, index,
	shard string, id strfmt.UUID, props search.SelectProperties,
	additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	return replica.Replica{}, nil
}

func (c *fakeReplicationClient) FetchObjects(ctx context.Context, host,
	index, shard string, ids []strfmt.UUID,
) ([]replica.Replica, error) {
	return nil, nil
}

func (c *fakeReplicationClient) DigestObjects(ctx context.Context,
	host, index, shard string, ids []strfmt.UUID, numRetries int,
) (result []types.RepairResponse, err error) {
	return nil, nil
}

func (c *fakeReplicationClient) OverwriteObjects(ctx context.Context,
	host, index, shard string, vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	return nil, nil
}

func (c *fakeReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filters *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	return nil, nil
}

func (c *fakeReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	return nil, nil
}

func (c *fakeReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string, level int,
	discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	return nil, nil
}
