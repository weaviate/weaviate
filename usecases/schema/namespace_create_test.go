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

package schema

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingcfg "github.com/weaviate/weaviate/usecases/sharding/config"
	"github.com/weaviate/weaviate/usecases/usagelimits"
	wlnamespaces "github.com/weaviate/weaviate/wl/namespaces"
)

// fakeNamespacesExister implements [namespaces.Exister] for tests that only
// need to seed a known namespace->home_node mapping. defaultHomeNode is the
// HomeNode returned for any name not in byName; setting it lets tests that
// don't care about per-namespace placement reuse this fake without seeding.
type fakeNamespacesExister struct {
	byName          map[string]cmd.Namespace
	defaultHomeNode string
}

func (f fakeNamespacesExister) GetNamespace(name string) (cmd.Namespace, bool) {
	if ns, ok := f.byName[name]; ok {
		return ns, true
	}
	if f.defaultHomeNode != "" {
		return cmd.Namespace{Name: name, HomeNodes: []string{f.defaultHomeNode}, State: cmd.NamespaceStateActive}, true
	}
	return cmd.Namespace{}, false
}

// homeNodes seeds an active namespace customer1 with nodes as its HomeNodes.
func homeNodes(nodes ...string) fakeNamespacesExister {
	return fakeNamespacesExister{byName: map[string]cmd.Namespace{
		"customer1": {Name: "customer1", HomeNodes: nodes, State: cmd.NamespaceStateActive},
	}}
}

// newTestHandlerWithNamespaces returns a Handler that has the cluster-wide
// NAMESPACES_ENABLED flag flipped on. It is otherwise identical to the helper
// at helpers_test.go:newTestHandler so existing fakes keep working.
func newTestHandlerWithNamespaces(t *testing.T, enabled bool) (*Handler, *fakeSchemaManager) {
	t.Helper()
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	cfg := config.Config{
		DefaultVectorDistanceMetric: "cosine",
	}
	cfg.Namespaces.Enabled = enabled
	qualifier := namespacing.Disabled
	if enabled {
		qualifier = wlnamespaces.NewPrefixing()
	}
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, dummyParseVectorConfig, fakeValidator, fakeModulesProvider{}, nil, nil)
	// Default exister returns HomeNode=node-1 for any name, matching the
	// default storage candidate set in fakeSchemaManager.StorageCandidates().
	// Tests that exercise placement should set handler.namespacesExister
	// directly to override this default.
	handler, err := NewHandler(
		schemaManager, schemaManager, schemaManager, &fakeDB{}, fakeValidator, logger, mocks.NewMockAuthorizer(),
		&cfg.SchemaHandlerConfig, cfg, dummyParseVectorConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, nil, *schemaParser, nil,
		fakeNamespacesExister{defaultHomeNode: "node-1"}, nil, qualifier)
	require.NoError(t, err)
	handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
	return &handler, schemaManager
}

func namespacedPrincipal(ns string) *models.Principal {
	return &models.Principal{Username: "u1", Namespace: ns}
}

func globalPrincipal() *models.Principal {
	return &models.Principal{Username: "admin", IsGlobalOperator: true}
}

func TestAddClass(t *testing.T) {
	t.Parallel()
	casts := []struct {
		name         string
		enabled      bool
		principal    *models.Principal
		class        string
		dataType     []string // DataType of a single "ref" property, nil for no properties
		wantClass    string
		wantDataType []string
		wantErrMsg   string
	}{
		{
			name:      "namespaced principal qualifies and persists prefixed class",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			class:     "Movies",
			wantClass: "customer1:Movies",
		},
		{
			name:         "namespaced principal's short ref target gets the class's namespace",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			class:        "Movies",
			dataType:     []string{"Animal"},
			wantClass:    "customer1:Movies",
			wantDataType: []string{"customer1:Animal"},
		},
		{
			name:       "namespaced principal's prefixed ref target rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			class:      "Movies",
			dataType:   []string{"customer2:Animal"},
			wantErrMsg: "'customer2:Animal' is not a valid class name",
		},
		{
			name:       "global principal rejected on namespaces enabled",
			enabled:    true,
			principal:  globalPrincipal(),
			class:      "Movies",
			wantErrMsg: "must be namespaced",
		},
		{
			name:       "nil principal rejected on namespaces enabled",
			enabled:    true,
			principal:  nil,
			class:      "Movies",
			wantErrMsg: "must be namespaced",
		},
		{
			name:      "global principal allowed on namespaces disabled",
			enabled:   false,
			principal: globalPrincipal(),
			class:     "Movies",
			wantClass: "Movies",
		},
		{
			name:       "namespaced principal short name containing colon rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			class:      "Customer2:Movies",
			wantErrMsg: "is not a valid class name",
		},
		{
			name:       "namespaced principal over length rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer"),
			class:      "C" + strings.Repeat("x", 254),
			wantErrMsg: "namespaced names must be at most",
		},
		{
			name:       "first letter whose uppercase is ASCII rejected",
			enabled:    false,
			principal:  globalPrincipal(),
			class:      "\u017fovies",
			wantErrMsg: "is not a valid class name",
		},
	}

	for _, tt := range casts {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			class := &models.Class{
				Class:             tt.class,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			if tt.dataType != nil {
				class.Properties = []*models.Property{{Name: "ref", DataType: tt.dataType}}
			}
			for _, target := range []string{"customer1:Animal", "customer2:Animal"} {
				sm.On("ReadOnlyClass", target).Return(&models.Class{Class: target}).Maybe()
			}

			if tt.wantClass != "" {
				sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
					if tt.wantDataType != nil && (len(c.Properties) != 1 || !slices.Equal(c.Properties[0].DataType, tt.wantDataType)) {
						return false
					}
					return c.Class == tt.wantClass
				}), mock.Anything).Return(nil)
			}

			got, _, err := handler.AddClass(context.Background(), tt.principal, class)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantClass, got.Class)
		})
	}
}

// TestAddClass_PinsShardsToNamespaceHomeNode pins a namespaced class to one
// shard owned by the first RF home nodes in stored order, and refuses a factor
// above their count. NS-disabled clusters place on the storage candidates.
func TestAddClass_PinsShardsToNamespaceHomeNode(t *testing.T) {
	t.Parallel()

	allCandidates := []string{"node-1", "node-2", "node-3"}

	tests := []struct {
		name      string
		enabled   bool
		principal *models.Principal
		exister   fakeNamespacesExister
		factor    int64
		storage   []string // defaults to allCandidates
		// wantOwners is the one shard's BelongsToNodes, in order.
		wantOwners []string
		wantErr    string
	}{
		{
			name:       "NS enabled: RF=2 takes the first two home nodes in stored order",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			exister:    homeNodes("node-3", "node-1", "node-2"),
			factor:     2,
			storage:    []string{"node-1", "node-2", "node-3", "node-4"},
			wantOwners: []string{"node-3", "node-1"},
		},
		{
			name:      "NS enabled: RF above the home-node count refused",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister:   homeNodes("node-1", "node-2"),
			factor:    3,
			wantErr:   "replication factor 3 exceeds the 2 home node(s) of the namespace",
		},
		{
			name:       "NS disabled: placed on the storage candidates",
			enabled:    false,
			principal:  globalPrincipal(),
			exister:    fakeNamespacesExister{},
			factor:     1,
			wantOwners: []string{"node-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)
			sm.storageCandidates = allCandidates
			if tt.storage != nil {
				sm.storageCandidates = tt.storage
			}
			handler.namespacesExister = tt.exister
			logger, hook := test.NewNullLogger()
			handler.logger = logger

			var capturedState *sharding.State
			sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
				return c != nil
			}), mock.MatchedBy(func(s *sharding.State) bool {
				capturedState = s
				return true
			})).Return(nil).Maybe()

			class := &models.Class{
				Class:             "Movies",
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: tt.factor},
			}
			_, _, err := handler.AddClass(context.Background(), tt.principal, class)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrValidation)
				assert.ErrorContains(t, err, tt.wantErr)
				assertErrorOmits(t, err, append([]string{"customer1"}, tt.exister.byName["customer1"].HomeNodes...)...)
				sm.AssertNotCalled(t, "AddClass", mock.Anything, mock.Anything)
				require.Len(t, hook.AllEntries(), 1)
				assert.Equal(t, logrus.WarnLevel, hook.LastEntry().Level)
				assert.Equal(t, "customer1", hook.LastEntry().Data["namespace"])
				return
			}
			require.NoError(t, err)

			require.NotNil(t, capturedState, "expected AddClass to be called with a sharding state")
			require.Len(t, capturedState.Physical, 1, "single non-MT shard expected")
			for _, phys := range capturedState.Physical {
				assert.Equal(t, tt.wantOwners, phys.BelongsToNodes)
			}
		})
	}
}

// assertErrorOmits fails when err holds any of names. A namespaced caller
// receives the placement and factor refusals, so they name neither the
// namespace nor a node.
func assertErrorOmits(t *testing.T, err error, names ...string) {
	t.Helper()
	for _, name := range names {
		assert.NotContains(t, err.Error(), name)
	}
}

// TestAddClass_NamespacePlacementErrors covers the failure modes that
// surface before the RAFT propose: missing namespace, missing home_nodes, or
// a home node that no longer belongs to the cluster's storage candidates.
func TestAddClass_NamespacePlacementErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		exister    fakeNamespacesExister
		candidates []string
		wantErr    string
		// wantLoggedMissing is the Warn line's home_nodes field, nil where the
		// line carries none.
		wantLoggedMissing []string
	}{
		{
			name:       "namespace not found",
			exister:    fakeNamespacesExister{}, // no seed
			candidates: []string{"node-1"},
			wantErr:    "namespace not found",
		},
		{
			name:       "home_nodes empty",
			exister:    homeNodes(),
			candidates: []string{"node-1"},
			wantErr:    "the namespace has no home_nodes; refusing placement",
		},
		{
			name:              "home_node not in storage candidates",
			exister:           homeNodes("node-1", "node-9"),
			candidates:        []string{"node-1"},
			wantErr:           "1 home node(s) of the namespace are not storage candidates",
			wantLoggedMissing: []string{"node-9"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, true)
			sm.storageCandidates = tt.candidates
			handler.namespacesExister = tt.exister
			logger, hook := test.NewNullLogger()
			handler.logger = logger

			class := &models.Class{
				Class:             "Movies",
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
			assertErrorOmits(t, err, "customer1", "node-1", "node-9")

			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			assert.Equal(t, logrus.WarnLevel, entry.Level)
			assert.Equal(t, "customer1", entry.Data["namespace"])
			if tt.wantLoggedMissing != nil {
				assert.Equal(t, tt.wantLoggedMissing, entry.Data["home_nodes"])
			}
		})
	}
}

// TestAddClass_RejectsExplicitDesiredCount asserts an explicit
// desiredCount != 1 is rejected before the propose runs on non-MT
// classes. MT classes can't reach this check because an upstream
// validator already rejects any ShardingConfig combined with a
// MultiTenancyConfig — covered separately by
// TestAddClass_MTRejectsShardingConfig.
func TestAddClass_RejectsExplicitDesiredCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		desiredCount any
		wantErrSub   string
	}{
		{name: "explicit 3", desiredCount: 3, wantErrSub: "desiredCount is limited to 1"},
		{name: "explicit 0", desiredCount: 0, wantErrSub: "desiredCount is limited to 1"},
		{name: "explicit 2 as float64 (JSON shape)", desiredCount: float64(2), wantErrSub: "desiredCount is limited to 1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, true)
			sm.storageCandidates = []string{"node-1", "node-2", "node-3"}
			handler.namespacesExister = fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
			}}

			class := &models.Class{
				Class:             "Movies",
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				ShardingConfig:    map[string]interface{}{"desiredCount": tt.desiredCount},
			}
			_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrSub)
			sm.AssertNotCalled(t, "AddClass", mock.Anything, mock.Anything)
		})
	}
}

// TestAddClass_MTRejectsShardingConfig locks in the existing constraint
// that MT classes cannot carry a ShardingConfig at all. This is what
// makes the desiredCount reject above safe to run uniformly: any MT class
// with a shardingConfig is rejected up-front before
// rejectExplicitMultiShardOnNamespacedClass would even be reached.
func TestAddClass_MTRejectsShardingConfig(t *testing.T) {
	t.Parallel()
	handler, sm := newTestHandlerWithNamespaces(t, true)
	sm.storageCandidates = []string{"node-1", "node-2", "node-3"}
	handler.namespacesExister = fakeNamespacesExister{byName: map[string]cmd.Namespace{
		"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
	}}

	class := &models.Class{
		Class:              "Movies",
		Vectorizer:         "model1",
		VectorIndexConfig:  map[string]interface{}{},
		ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		ShardingConfig:     map[string]interface{}{"desiredCount": 3},
	}
	_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot have both shardingConfig and multiTenancyConfig")
}

// TestAddClass_AcceptsExplicitDesiredCountOne accepts the only compatible
// explicit value.
func TestAddClass_AcceptsExplicitDesiredCountOne(t *testing.T) {
	t.Parallel()

	handler, sm := newTestHandlerWithNamespaces(t, true)
	sm.storageCandidates = []string{"node-1", "node-2", "node-3"}
	handler.namespacesExister = fakeNamespacesExister{byName: map[string]cmd.Namespace{
		"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
	}}
	sm.On("AddClass", mock.Anything, mock.Anything).Return(nil)

	class := &models.Class{
		Class:             "Movies",
		Vectorizer:        "model1",
		VectorIndexConfig: map[string]interface{}{},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		ShardingConfig:    map[string]interface{}{"desiredCount": 1},
	}
	_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)
	require.NoError(t, err)
}

// TestAddClass_OmittedShardingConfigPinsToHomeNode covers the implicit
// path: no ShardingConfig → parser default → override caps to 1.
func TestAddClass_OmittedShardingConfigPinsToHomeNode(t *testing.T) {
	t.Parallel()

	handler, sm := newTestHandlerWithNamespaces(t, true)
	sm.storageCandidates = []string{"node-1", "node-2", "node-3"}
	handler.namespacesExister = fakeNamespacesExister{byName: map[string]cmd.Namespace{
		"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
	}}

	var capturedClass *models.Class
	var capturedState *sharding.State
	sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
		capturedClass = c
		return c != nil
	}), mock.MatchedBy(func(s *sharding.State) bool {
		capturedState = s
		return true
	})).Return(nil)

	class := &models.Class{
		Class:             "Movies",
		Vectorizer:        "model1",
		VectorIndexConfig: map[string]interface{}{},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)
	require.NoError(t, err)

	require.NotNil(t, capturedClass)
	got, ok := capturedClass.ShardingConfig.(shardingcfg.Config)
	require.True(t, ok, "ShardingConfig should be shardingcfg.Config, got %T", capturedClass.ShardingConfig)
	assert.Equal(t, 1, got.DesiredCount, "a namespaced class has one shard")
	assert.Equal(t, got.DesiredCount, got.ActualCount)
	assert.Equal(t, got.DesiredCount*got.VirtualPerPhysical, got.DesiredVirtualCount)

	require.NotNil(t, capturedState)
	require.Len(t, capturedState.Physical, 1, "expected one physical shard after override")
	for _, phys := range capturedState.Physical {
		assert.Equal(t, []string{"node-2"}, phys.BelongsToNodes)
	}
}

// TestAddClass_ShardCapAppliesAfterOverride regression-guards the
// ordering: shard cap must evaluate the post-override DesiredCount.
func TestAddClass_ShardCapAppliesAfterOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		shardCap      int
		expectExceeds bool
	}{
		{name: "cap=1, post-override DesiredCount=1 fits", shardCap: 1, expectExceeds: false},
		{name: "cap=0, post-override DesiredCount=1 still over", shardCap: 0, expectExceeds: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, true)
			sm.storageCandidates = []string{"node-1", "node-2", "node-3"}
			handler.namespacesExister = fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
			}}
			handler.config.UsageLimits.MaxShardsPerCollection = runtime.NewDynamicValue(tt.shardCap)

			if !tt.expectExceeds {
				sm.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			}

			class := &models.Class{
				Class:             "Movies",
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)

			if tt.expectExceeds {
				require.Error(t, err)
				le, ok := usagelimits.AsLimitExceeded(err)
				require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
				assert.Equal(t, usagelimits.LimitShards, le.Limit)
				assert.Equal(t, int64(tt.shardCap), le.Value)
			} else {
				require.NoError(t, err, "override should reduce DesiredCount to 1 before the cap check")
			}
		})
	}
}

// TestAddTenants_PinsShardsToNamespaceHomeNode checks that a tenant create
// sends nil ClusterNodes on NS-enabled clusters, for apply to fill from
// HomeNodes, and every storage candidate on NS-disabled clusters.
func TestAddTenants_PinsShardsToNamespaceHomeNode(t *testing.T) {
	t.Parallel()

	allCandidates := []string{"node-1", "node-2", "node-3"}

	tests := []struct {
		name      string
		enabled   bool
		principal *models.Principal
		exister   fakeNamespacesExister
		class     string
		wantNodes []string
		wantErr   string
	}{
		{
			name:      "NS enabled: ClusterNodes left for apply to fill",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister:   homeNodes("node-2"),
			class:     "Movies",
		},
		{
			name:      "NS enabled: a home node outside the storage candidates is refused",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister:   homeNodes("node-1", "node-9"),
			class:     "Movies",
			wantErr:   "1 home node(s) of the namespace are not storage candidates",
		},
		{
			name:      "NS disabled: ClusterNodes is the full candidate set",
			enabled:   false,
			principal: globalPrincipal(),
			exister:   fakeNamespacesExister{},
			class:     "Movies",
			wantNodes: allCandidates,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)
			sm.storageCandidates = allCandidates
			handler.namespacesExister = tt.exister

			var captured *cmd.AddTenantsRequest
			sm.On("AddTenants", mock.Anything, mock.MatchedBy(func(req *cmd.AddTenantsRequest) bool {
				captured = req
				return true
			})).Return(nil)

			_, err := handler.AddTenants(context.Background(), tt.principal, tt.class,
				[]*models.Tenant{{Name: "T1"}, {Name: "T2"}})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assertErrorOmits(t, err, "customer1", "node-9")
				assert.Nil(t, captured, "nothing may be proposed")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, captured, "expected AddTenants to be called")
			assert.Equal(t, tt.wantNodes, captured.ClusterNodes)
		})
	}
}

// TestUpdateTenants_PinsShardsToNamespaceHomeNode checks that a tenant update
// sends nil ClusterNodes on NS-enabled clusters, for SchemaManager.UpdateTenants
// to fill from HomeNodes at apply, and every storage candidate on NS-disabled clusters.
func TestUpdateTenants_PinsShardsToNamespaceHomeNode(t *testing.T) {
	t.Parallel()

	allCandidates := []string{"node-1", "node-2", "node-3"}

	tests := []struct {
		name      string
		enabled   bool
		principal *models.Principal
		exister   fakeNamespacesExister
		class     string
		wantNodes []string
	}{
		{
			name:      "NS enabled: ClusterNodes left for apply to fill",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister:   homeNodes("node-2"),
			class:     "Movies",
		},
		{
			name:      "NS disabled: ClusterNodes is the full candidate set",
			enabled:   false,
			principal: globalPrincipal(),
			exister:   fakeNamespacesExister{},
			class:     "Movies",
			wantNodes: allCandidates,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)
			sm.storageCandidates = allCandidates
			handler.namespacesExister = tt.exister

			var captured *cmd.UpdateTenantsRequest
			sm.On("UpdateTenants", mock.Anything, mock.MatchedBy(func(req *cmd.UpdateTenantsRequest) bool {
				captured = req
				return true
			})).Return(nil)
			sm.On("TenantsShardsStatusWithVersion", mock.Anything, uint64(0), mock.Anything, mock.Anything).
				Return(map[string]string{"T1": models.TenantActivityStatusHOT}, nil)

			_, err := handler.UpdateTenants(context.Background(), tt.principal, tt.class,
				[]*models.Tenant{{Name: "T1", ActivityStatus: models.TenantActivityStatusHOT}})
			require.NoError(t, err)
			require.NotNil(t, captured, "expected UpdateTenants to be called")
			assert.Equal(t, tt.wantNodes, captured.ClusterNodes)
		})
	}
}

func TestAddAlias(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		enabled    bool
		principal  *models.Principal
		alias      string
		class      string
		wantAlias  string
		wantClass  string
		wantErrMsg string
		mockTarget string
	}{
		{
			name:       "namespaced principal qualifies both",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			alias:      "Films",
			class:      "Movies",
			wantAlias:  "customer1:Films",
			wantClass:  "customer1:Movies",
			mockTarget: "customer1:Movies",
		},
		{
			name:       "global principal rejected on namespaces enabled",
			enabled:    true,
			principal:  globalPrincipal(),
			alias:      "Films",
			class:      "Movies",
			wantErrMsg: "must be namespaced",
		},
		{
			name:       "namespaced principal colon in target rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			alias:      "Films",
			class:      "Customer2:Movies",
			wantErrMsg: "is not a valid class name",
		},
		{
			name:       "namespaced principal colon in alias rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			alias:      "Customer2:Films",
			class:      "Movies",
			wantErrMsg: "is not a valid alias name",
		},
		{
			name:       "alias first letter whose uppercase is ASCII rejected",
			enabled:    false,
			principal:  globalPrincipal(),
			alias:      "\u0131tems",
			class:      "Movies",
			wantErrMsg: "is not a valid alias name",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			if tt.mockTarget != "" {
				target := &models.Class{Class: tt.mockTarget}
				sm.On("ReadOnlyClass", tt.mockTarget).Return(target)
				sm.On("CreateAlias", mock.Anything, tt.wantAlias, mock.MatchedBy(func(c *models.Class) bool {
					return c != nil && c.Class == tt.wantClass
				})).Return(nil)
			}

			alias := &models.Alias{Alias: tt.alias, Class: tt.class}
			got, _, err := handler.AddAlias(context.Background(), tt.principal, alias)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAlias, got.Alias)
			require.Equal(t, tt.wantClass, got.Class)
		})
	}
}

func TestUpdateAlias_RejectsCrossNamespaceTarget(t *testing.T) {
	t.Parallel()
	handler, sm := newTestHandlerWithNamespaces(t, true)
	sm.On("AliasesFromLeader", mock.Anything, "customer1:Films", mock.Anything).
		Return([]*models.Alias{{Alias: "customer1:Films", Class: "customer1:Movies"}}, nil)
	sm.On("ReadOnlyClass", "customer2:Secret").Return(&models.Class{Class: "customer2:Secret"})
	sm.On("ReplaceAlias", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	_, err := handler.UpdateAlias(context.Background(), globalPrincipal(), "customer1:Films", "customer2:Secret")
	require.ErrorIs(t, err, ErrValidation)
	assert.ErrorContains(t, err, "are in different namespaces")
	sm.AssertNotCalled(t, "ReplaceAlias", mock.Anything, mock.Anything, mock.Anything)
}

// TestUpdateClass_QualifiesPropertyDataTypes pins the GET→PUT round-trip.
// A short cross-ref DataType gets the class's namespace before it reaches the
// SchemaManager, and a target in another namespace is rejected for every caller.
func TestUpdateClass_QualifiesPropertyDataTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		enabled       bool
		principal     *models.Principal
		storedClass   string
		storedDT      []string // DataType the stored class holds, nil if wantStoredDT
		bodyClass     string
		bodyDataType  []string
		nilBody       bool
		wantStoredDT  []string
		wantErrSubstr string
	}{
		{
			name:         "namespaced: stripped cross-ref DataType qualified before SchemaManager",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			storedClass:  "customer1:Movies",
			bodyClass:    "Movies",
			bodyDataType: []string{"Other"},
			wantStoredDT: []string{"customer1:Other"},
		},
		{
			name:         "namespaced: multi-target stripped refs all qualified",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			storedClass:  "customer1:Movies",
			bodyClass:    "Movies",
			bodyDataType: []string{"Other", "Another"},
			wantStoredDT: []string{"customer1:Other", "customer1:Another"},
		},
		{
			name:          "namespaced: foreign-namespace ref DataType rejected",
			enabled:       true,
			principal:     namespacedPrincipal("customer1"),
			storedClass:   "customer1:Movies",
			bodyClass:     "Movies",
			bodyDataType:  []string{"customer2:Other"},
			wantErrSubstr: "is not a valid class name",
		},
		{
			name:         "namespaced: primitive DataType passes through untouched",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			storedClass:  "customer1:Movies",
			bodyClass:    "Movies",
			bodyDataType: []string{"text"},
			wantStoredDT: []string{"text"},
		},
		{
			name:         "global: short cross-ref DataType gets the class's namespace",
			enabled:      true,
			principal:    globalPrincipal(),
			storedClass:  "customer1:Movies",
			bodyClass:    "customer1:Movies",
			bodyDataType: []string{"Other"},
			wantStoredDT: []string{"customer1:Other"},
		},
		{
			name:         "global: cross-ref DataType in the class's namespace kept",
			enabled:      true,
			principal:    globalPrincipal(),
			storedClass:  "customer1:Movies",
			bodyClass:    "customer1:Movies",
			bodyDataType: []string{"customer1:Other"},
			wantStoredDT: []string{"customer1:Other"},
		},
		{
			name:          "global: foreign-namespace ref DataType rejected",
			enabled:       true,
			principal:     globalPrincipal(),
			storedClass:   "customer1:Movies",
			bodyClass:     "customer1:Movies",
			bodyDataType:  []string{"customer2:Other"},
			wantErrSubstr: "'customer2:Other' is not a valid class name",
		},
		{
			name:          "global: stored foreign-namespace ref DataType sent back unchanged rejected",
			enabled:       true,
			principal:     globalPrincipal(),
			storedClass:   "customer1:Movies",
			storedDT:      []string{"customer2:Other"},
			bodyClass:     "customer1:Movies",
			bodyDataType:  []string{"customer2:Other"},
			wantErrSubstr: "'customer2:Other' is not a valid class name",
		},
		{
			name:        "global: nil body returns without reaching SchemaManager",
			enabled:     true,
			principal:   globalPrincipal(),
			storedClass: "customer1:Movies",
			bodyClass:   "customer1:Movies",
			nilBody:     true,
		},
		{
			name:        "namespaced: nil body returns without reaching SchemaManager",
			enabled:     true,
			principal:   namespacedPrincipal("customer1"),
			storedClass: "customer1:Movies",
			bodyClass:   "Movies",
			nilBody:     true,
		},
		{
			name:         "NS disabled: qualifier is a no-op, body passes through verbatim",
			enabled:      false,
			principal:    nil,
			storedClass:  "Movies",
			bodyClass:    "Movies",
			bodyDataType: []string{"Other"},
			wantStoredDT: []string{"Other"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			storedDT := tt.storedDT
			if storedDT == nil {
				storedDT = tt.wantStoredDT
			}
			stored := &models.Class{
				Class:             tt.storedClass,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				Properties: []*models.Property{
					{Name: "watched", DataType: storedDT},
				},
			}
			sm.On("ReadOnlyClass", tt.storedClass).Return(stored).Maybe()
			sm.On("ReadOnlyClassesFromLeader", mock.Anything).Return(map[string]versioned.Class{}, nil).Maybe()

			var captured *models.Class
			sm.On("UpdateClass", mock.MatchedBy(func(c *models.Class) bool {
				captured = c
				return true
			}), mock.Anything, cmd.ClassUpdateOriginUser).Return(nil).Maybe()

			body := &models.Class{
				Class:             tt.bodyClass,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				Properties: []*models.Property{
					{Name: "watched", DataType: tt.bodyDataType},
				},
			}
			if tt.nilBody {
				body = nil
			}

			err := handler.UpdateClass(context.Background(), tt.principal, tt.bodyClass, body)
			if tt.nilBody {
				require.NoError(t, err)
				require.Nil(t, captured, "SchemaManager.UpdateClass must not be called without a body")
				return
			}
			if tt.wantErrSubstr != "" {
				require.ErrorIs(t, err, ErrValidation)
				assert.Contains(t, err.Error(), tt.wantErrSubstr)
				require.Nil(t, captured, "SchemaManager.UpdateClass must not be called on rejected body")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, captured, "SchemaManager.UpdateClass should have been called")
			require.Equal(t, tt.storedClass, captured.Class)
			require.Len(t, captured.Properties, 1)
			assert.Equal(t, tt.wantStoredDT, captured.Properties[0].DataType)
		})
	}
}

// TestUpdateClass_NamespaceFactorAboveHomeNodes pins that UpdateClass refuses
// a factor above the namespace's HomeNodes count before anything is proposed.
func TestUpdateClass_NamespaceFactorAboveHomeNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		exister fakeNamespacesExister
		mt      bool
		// owners is the one shard's BelongsToNodes of a class that is not MT.
		owners  []string
		factor  int64
		wantErr string
	}{
		{
			name:    "MT class with no tenants raised above the home-node count",
			exister: homeNodes("node-1"),
			mt:      true,
			factor:  3,
			wantErr: "replication factor 3 exceeds the 1 home node(s) of the namespace",
		},
		{
			name:    "class with one shard raised above the home-node count",
			exister: homeNodes("node-1"),
			owners:  []string{"node-1"},
			factor:  2,
			wantErr: "replication factor 2 exceeds the 1 home node(s) of the namespace",
		},
		{
			name:    "MT class whose namespace is not found raised",
			exister: fakeNamespacesExister{},
			mt:      true,
			factor:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, true)
			handler.namespacesExister = tt.exister
			principal, bodyName, storedName := namespacedPrincipal("customer1"), "Movies", "customer1:Movies"

			class := func(name string, factor int64) *models.Class {
				return &models.Class{
					Class:              name,
					Vectorizer:         "model1",
					VectorIndexConfig:  map[string]interface{}{},
					ReplicationConfig:  &models.ReplicationConfig{Factor: factor},
					MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: tt.mt},
				}
			}
			stored := class(storedName, 1)
			state := &sharding.State{PartitioningEnabled: tt.mt, Physical: map[string]sharding.Physical{}}
			if tt.owners != nil {
				state.Physical["S1"] = sharding.Physical{Name: "S1", BelongsToNodes: tt.owners}
			}
			sm.On("ReadOnlyClass", storedName).Return(stored).Maybe()
			sm.On("ReadOnlyClassesFromLeader", mock.Anything).Return(map[string]versioned.Class{}, nil).Maybe()
			sm.On("Read", storedName, mock.Anything).Run(func(args mock.Arguments) {
				reader := args.Get(1).(func(*models.Class, *sharding.State) error)
				_ = reader(stored, state)
			}).Return(nil).Maybe()
			sm.On("UpdateClass", mock.Anything, mock.Anything, cmd.ClassUpdateOriginUser).Return(nil).Maybe()

			err := handler.UpdateClass(context.Background(), principal, bodyName, class(bodyName, tt.factor))
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrValidation)
				assert.ErrorContains(t, err, tt.wantErr)
				assertErrorOmits(t, err, append([]string{"customer1"}, tt.exister.byName["customer1"].HomeNodes...)...)
				sm.AssertNotCalled(t, "UpdateClass", mock.Anything, mock.Anything, mock.Anything)
				return
			}
			require.NoError(t, err)
			sm.AssertCalled(t, "UpdateClass", mock.Anything, mock.Anything, cmd.ClassUpdateOriginUser)
		})
	}
}

// TestAddClass_NamespacedCollectionLimit checks that the
// MAXIMUM_ALLOWED_COLLECTIONS_COUNT cap is enforced per namespace when
// namespaces are enabled, using principal.Namespace as the selector.
//
// The mock matcher on CollectionsCount(<principalNS>) is what proves
// the selector: a row only passes if AddClass requested the count for the
// caller's namespace.
func TestAddClass_NamespacedCollectionLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		principalNS   string
		limit         int
		existingCount int
		wantErr       bool
	}{
		{
			name:          "same namespace at cap rejects",
			principalNS:   "customer1",
			limit:         1,
			existingCount: 1,
			wantErr:       true,
		},
		{
			name:          "under cap succeeds",
			principalNS:   "customer1",
			limit:         1,
			existingCount: 0,
			wantErr:       false,
		},
		{
			name:          "different namespace under cap succeeds",
			principalNS:   "customer2",
			limit:         10,
			existingCount: 0,
			wantErr:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, true)
			handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(tt.limit)

			sm.On("CollectionsCount", tt.principalNS).Return(tt.existingCount)
			if !tt.wantErr {
				sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
					return c.Class == tt.principalNS+":Movies"
				}), mock.Anything).Return(nil)
			}

			class := &models.Class{
				Class:             "Movies",
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			_, _, err := handler.AddClass(context.Background(), namespacedPrincipal(tt.principalNS), class)

			if tt.wantErr {
				require.Error(t, err)
				le, ok := usagelimits.AsLimitExceeded(err)
				require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
				assert.Equal(t, usagelimits.LimitCollections, le.Limit)
				assert.Equal(t, int64(tt.limit), le.Value)
			} else {
				require.NoError(t, err)
			}
			sm.AssertExpectations(t)
		})
	}
}

// TestAddClass_NamespaceChecksRunAfterAuthorize pins that a namespaced caller
// the authorizer denies gets that denial, not a placement or factor refusal.
func TestAddClass_NamespaceChecksRunAfterAuthorize(t *testing.T) {
	t.Parallel()
	handler, sm := newTestHandlerWithNamespaces(t, true)
	handler.namespacesExister = homeNodes("node-1")
	denied := errors.New("denied")
	authorizer := mocks.NewMockAuthorizer()
	authorizer.SetErr(denied)
	handler.Authorizer = authorizer

	_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), &models.Class{
		Class:             "Movies",
		Vectorizer:        "model1",
		VectorIndexConfig: map[string]interface{}{},
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
	})
	require.ErrorIs(t, err, denied)
	sm.AssertNotCalled(t, "AddClass", mock.Anything, mock.Anything)
}
