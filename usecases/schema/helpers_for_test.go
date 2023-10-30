//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/cloud/store"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func newTestHandler(t *testing.T, db store.DB) (*Handler, func() raft.Future) {
	cfg := config.Config{}
	cluster, writer, clusterstate := startRaftCluster(t)
	writer.SetDB(db)
	reader := writer.SchemaReader()
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{
			"model1", "model2",
		},
	}
	handler, err := NewHandler(
		writer, reader, &fakeValidator{}, logger, &fakeAuthorizer{nil},
		cfg, dummyParseVectorConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, clusterstate, &fakeScaleOutManager{})
	require.Nil(t, err)
	return &handler, cluster.Shutdown
}

func startRaftCluster(t *testing.T) (*store.Cluster, *store.Store, clusterState) {
	node := "node-1"
	url := newRandomHostURL(t)
	candidates := []store.Candidate{
		{
			ID:       node,
			Address:  url.address,
			NonVoter: false,
		},
	}

	root := t.TempDir()
	clusterstate := newFakeClusterState()

	writer := store.New(store.Config{
		WorkDir:              root,
		NodeID:               node,
		Host:                 url.host,
		RaftPort:             newRandomHostURL(t).port,
		RaftElectionTimeout:  500 * time.Millisecond,
		RaftHeartbeatTimeout: 500 * time.Millisecond,
		Parser:               NewParser(clusterstate, dummyParseVectorConfig),
	})

	raftNode, err := writer.Open(false, candidates)
	require.Nil(t, err)
	cluster := store.NewCluster(raftNode, fmt.Sprintf("%s:%d", url.host, url.port))
	err = cluster.Open()
	require.Nil(t, err, "expected nil error, got: %v", err)
	// Allow time to elect leader
	time.Sleep(2 * time.Second)
	return &cluster, &writer, clusterstate
}

type randomHostURL struct {
	address string
	host    string
	port    int
}

func newRandomHostURL(t *testing.T) randomHostURL {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {}),
	)
	defer srv.Close()
	split := strings.Split(srv.URL, ":")
	host := strings.TrimPrefix(split[1], "//")
	port, err := strconv.Atoi(split[2])
	require.Nil(t, err)
	return randomHostURL{
		address: srv.URL,
		host:    host,
		port:    port,
	}
}

type fakeDB struct {
	mock.Mock
}

func (f *fakeDB) AddClass(cmd command.AddClassRequest) error {
	return nil
}

func (f *fakeDB) UpdateClass(cmd command.UpdateClassRequest) error {
	return nil
}

func (f *fakeDB) DeleteClass(class string) error {
	return nil
}

func (f *fakeDB) AddProperty(prop string, cmd command.AddPropertyRequest) error {
	return nil
}

func (f *fakeDB) AddTenants(class string, cmd *command.AddTenantsRequest) error {
	return nil
}
func (f *fakeDB) UpdateTenants(class string, cmd *command.UpdateTenantsRequest) error {
	return nil
}

func (f *fakeDB) DeleteTenants(class string, cmd *command.DeleteTenantsRequest) error {
	return nil
}

func (f *fakeDB) UpdateShardStatus(cmd *command.UpdateShardStatusRequest) error {
	return nil
}

func (f *fakeDB) GetShardsStatus(class string) (models.ShardStatusList, error) {
	args := f.Called(class)
	return args.Get(0).(models.ShardStatusList), nil
}

type fakeAuthorizer struct {
	err error
}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return f.err
}

type fakeScaleOutManager struct{}

func (f *fakeScaleOutManager) Scale(ctx context.Context,
	className string, updated sharding.Config, _, _ int64,
) (*sharding.State, error) {
	return nil, nil
}

func (f *fakeScaleOutManager) SetSchemaManager(sm scaler.SchemaManager) {
}

type fakeShardReader struct {
	mock.Mock
}

func (f *fakeShardReader) GetShardsStatus(class string) (models.ShardStatusList, error) {
	args := f.Called(class)
	return args.Get(0).(models.ShardStatusList), nil
}

type fakeValidator struct{}

func (f *fakeValidator) ValidateVectorIndexConfigUpdate(ctx context.Context,
	old, updated schema.VectorIndexConfig,
) error {
	return nil
}

func (f *fakeValidator) ValidateInvertedIndexConfigUpdate(ctx context.Context,
	old, updated *models.InvertedIndexConfig,
) error {
	return nil
}

type fakeModuleConfig struct{}

func (f *fakeModuleConfig) SetClassDefaults(class *models.Class) {
	defaultConfig := map[string]interface{}{
		"my-module1": map[string]interface{}{
			"my-setting": "default-value",
		},
	}

	asMap, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		class.ModuleConfig = defaultConfig
		return
	}

	module, ok := asMap["my-module1"]
	if !ok {
		class.ModuleConfig = defaultConfig
		return
	}

	asMap, ok = module.(map[string]interface{})
	if !ok {
		class.ModuleConfig = defaultConfig
		return
	}

	if _, ok := asMap["my-setting"]; !ok {
		asMap["my-setting"] = "default-value"
		defaultConfig["my-module1"] = asMap
		class.ModuleConfig = defaultConfig
	}
}

func (f *fakeModuleConfig) SetSinglePropertyDefaults(class *models.Class,
	prop *models.Property,
) {
}

func (f *fakeModuleConfig) ValidateClass(ctx context.Context, class *models.Class) error {
	return nil
}

type fakeVectorizerValidator struct {
	valid []string
}

func (f *fakeVectorizerValidator) ValidateVectorizer(moduleName string) error {
	for _, valid := range f.valid {
		if moduleName == valid {
			return nil
		}
	}

	return fmt.Errorf("invalid vectorizer %q", moduleName)
}

type fakeClusterState struct {
	hosts       []string
	syncIgnored bool
	skipRepair  bool
}

func newFakeClusterState(hosts ...string) *fakeClusterState {
	return &fakeClusterState{
		hosts: func() []string {
			if len(hosts) == 0 {
				return []string{"node-1"}
			}
			return hosts
		}(),
	}
}

func (f *fakeClusterState) SchemaSyncIgnored() bool {
	return f.syncIgnored
}

func (f *fakeClusterState) SkipSchemaRepair() bool {
	return f.skipRepair
}

func (f *fakeClusterState) Hostnames() []string {
	return f.hosts
}

func (f *fakeClusterState) AllNames() []string {
	return f.hosts
}

func (f *fakeClusterState) Candidates() []string {
	return f.hosts
}

func (f *fakeClusterState) LocalName() string {
	return "node1"
}

func (f *fakeClusterState) NodeCount() int {
	return 1
}

func (f *fakeClusterState) ClusterHealthScore() int {
	return 0
}

func (f *fakeClusterState) ResolveParentNodes(string, string,
) (map[string]string, error) {
	return nil, nil
}

func (f *fakeClusterState) NodeHostname(string) (string, bool) {
	return "", false
}

type fakeVectorConfig struct {
	raw interface{}
}

func (f fakeVectorConfig) IndexType() string {
	return "fake"
}

func dummyParseVectorConfig(in interface{}) (schema.VectorIndexConfig, error) {
	return fakeVectorConfig{raw: in}, nil
}

func dummyValidateInvertedConfig(in *models.InvertedIndexConfig) error {
	return nil
}

type fakeMigrator struct{}

func (f *fakeMigrator) AddClass(ctx context.Context, class *models.Class, shardingState *sharding.State) error {
	return nil
}

func (f *fakeMigrator) DropClass(ctx context.Context, className string) error {
	return nil
}

func (f *fakeMigrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	return nil
}

func (f *fakeMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	return nil
}

func (f *fakeMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	return nil
}

func (f *fakeMigrator) NewTenants(ctx context.Context, class *models.Class, creates []*CreateTenantPayload) (commit func(success bool), err error) {
	return nil, nil
}

func (f *fakeMigrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*UpdateTenantPayload) (commit func(success bool), err error) {
	return nil, nil
}

func (f *fakeMigrator) DeleteTenants(ctx context.Context, class string, tenants []string) (commit func(success bool), err error) {
	return nil, nil
}

func (f *fakeMigrator) GetShardsStatus(ctx context.Context, className string) (map[string]string, error) {
	return nil, nil
}

func (f *fakeMigrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	return nil
}

func (f *fakeMigrator) ValidateVectorIndexConfigUpdate(ctx context.Context, old, updated schema.VectorIndexConfig) error {
	return nil
}

func (f *fakeMigrator) UpdateVectorIndexConfig(ctx context.Context, className string, updated schema.VectorIndexConfig) error {
	return nil
}

func (f *fakeMigrator) ValidateInvertedIndexConfigUpdate(ctx context.Context, old, updated *models.InvertedIndexConfig) error {
	return nil
}

func (f *fakeMigrator) UpdateInvertedIndexConfig(ctx context.Context, className string, updated *models.InvertedIndexConfig) error {
	return nil
}

type configMigrator struct {
	fakeMigrator
	vectorConfigValidationError    error
	vectorConfigValidateCalledWith schema.VectorIndexConfig
	vectorConfigUpdateCalled       bool
	vectorConfigUpdateCalledWith   schema.VectorIndexConfig
}

func (m *configMigrator) ValidateVectorIndexConfigUpdate(ctx context.Context,
	old, updated schema.VectorIndexConfig,
) error {
	m.vectorConfigValidateCalledWith = updated
	return m.vectorConfigValidationError
}

func (m *configMigrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schema.VectorIndexConfig,
) error {
	m.vectorConfigUpdateCalledWith = updated
	m.vectorConfigUpdateCalled = true
	return nil
}

func newSchemaManager() *Manager {
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	dummyConfig := config.Config{
		DefaultVectorizerModule:     config.VectorizerModuleNone,
		DefaultVectorDistanceMetric: "cosine",
	}
	sm, err := NewManager(&fakeMigrator{}, nil, nil, newFakeRepo(), logger, &fakeAuthorizer{},
		dummyConfig, dummyParseVectorConfig, // only option for now
		vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, &fakeClusterState{hosts: []string{"node1"}},
		nil, nil, &fakeScaleOutManager{},
	)
	if err != nil {
		panic(err.Error())
	}

	sm.StartServing(context.Background()) // will also mark tx manager as ready

	return sm
}

type fakeRepo struct {
	schema State
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		schema: NewState(1),
	}
}

func (f *fakeRepo) Save(ctx context.Context, schema State) error {
	f.schema = schema
	return nil
}

func (f *fakeRepo) Load(context.Context) (State, error) {
	return f.schema, nil
}

func (f *fakeRepo) NewClass(context.Context, ClassPayload) error {
	return nil
}

func (f *fakeRepo) UpdateClass(context.Context, ClassPayload) error {
	return nil
}

func (f *fakeRepo) DeleteClass(ctx context.Context, class string) error {
	return nil
}

func (f *fakeRepo) NewShards(ctx context.Context, class string, shards []KeyValuePair) error {
	return nil
}

func (f *fakeRepo) UpdateShards(ctx context.Context, class string, shards []KeyValuePair) error {
	return nil
}

func (f *fakeRepo) DeleteShards(ctx context.Context, class string, shards []string) error {
	return nil
}
