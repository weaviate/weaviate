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

package hybridtest

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/file"
	"github.com/weaviate/weaviate/usecases/objects"
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

func (f *fakeSchemaGetter) ReadOnlyClass(class string) *models.Class {
	return f.schema.GetClass(class)
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
	return x.BelongsToNodes[0], nil
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

func (f *fakeSchemaGetter) NodeName() string {
	return "node1"
}

func (f *fakeSchemaGetter) ClusterHealthScore() int {
	return 0
}

func (f fakeSchemaGetter) ResolveParentNodes(_ string, shard string) (map[string]string, error) {
	return nil, nil
}

func (f fakeSchemaGetter) Statistics() map[string]any {
	return nil
}

func randomVector(r *rand.Rand, dim int) []float32 {
	out := make([]float32, dim)
	for i := range out {
		out[i] = r.Float32()
	}

	return out
}

func singleShardState() *sharding.State {
	config, err := shardingConfig.ParseConfig(nil, 1)
	if err != nil {
		panic(err)
	}

	selector := mocks.NewMockNodeSelector("node1")
	s, err := sharding.InitState("test-index", config, selector.LocalName(), selector.StorageCandidates(), 1, false)
	if err != nil {
		panic(err)
	}

	return s
}

func getRandomSeed() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

type fakeRemoteClient struct{}

func (f *fakeRemoteClient) BatchPutObjects(ctx context.Context, hostName, indexName, shardName string, objs []*storobj.Object, repl *additional.ReplicationProperties, schemaVersion uint64) []error {
	return nil
}

func (f *fakeRemoteClient) PutObject(ctx context.Context, hostName, indexName,
	shardName string, obj *storobj.Object, schemaVersion uint64,
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
	shardName string, id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) MergeObject(ctx context.Context, hostName, indexName,
	shardName string, mergeDoc objects.MergeDocument, schemaVersion uint64,
) error {
	return nil
}

func (f *fakeRemoteClient) MultiGetObjects(ctx context.Context, hostName, indexName,
	shardName string, ids []strfmt.UUID,
) ([]*storobj.Object, error) {
	return nil, nil
}

func (f *fakeRemoteClient) SearchShard(ctx context.Context, hostName, indexName,
	shardName string, vector []models.Vector, targetVector []string, distance float32, limit int,
	filters *filters.LocalFilter, _ *searchparams.KeywordRanking, sort []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy, additional additional.Properties, targetCombination *dto.TargetCombination,
	properties []string,
) ([]*storobj.Object, []float32, error) {
	return nil, nil, nil
}

func (f *fakeRemoteClient) Aggregate(ctx context.Context, hostName, indexName,
	shardName string, params aggregation.Params,
) (*aggregation.Result, error) {
	return nil, nil
}

func (f *fakeRemoteClient) BatchAddReferences(ctx context.Context, hostName,
	indexName, shardName string, refs objects.BatchReferences, schemaVersion uint64,
) []error {
	return nil
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

func (f *fakeRemoteClient) PutFile(ctx context.Context, hostName, indexName, shardName,
	fileName string, payload io.ReadSeekCloser,
) error {
	return nil
}

func (f *fakeRemoteClient) PauseFileActivity(ctx context.Context, hostName, indexName, shardName string, schemaVersion uint64) error {
	return nil
}

func (f *fakeRemoteClient) ResumeFileActivity(ctx context.Context, hostName, indexName, shardName string) error {
	return nil
}

func (f *fakeRemoteClient) ListFiles(ctx context.Context, hostName, indexName, shardName string) ([]string, error) {
	return nil, nil
}

func (f *fakeRemoteClient) GetFileMetadata(ctx context.Context, hostName, indexName, shardName,
	fileName string,
) (file.FileMetadata, error) {
	return file.FileMetadata{}, nil
}

func (f *fakeRemoteClient) GetFile(ctx context.Context, hostName, indexName, shardName,
	fileName string,
) (io.ReadCloser, error) {
	return nil, nil
}

func (f *fakeRemoteClient) AddAsyncReplicationTargetNode(ctx context.Context, hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error {
	return nil
}

func (f *fakeRemoteClient) RemoveAsyncReplicationTargetNode(ctx context.Context, hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	return nil
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
