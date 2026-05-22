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
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingcfg "github.com/weaviate/weaviate/usecases/sharding/config"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// fakeNamespacesExister implements [namespaces.Exister] for tests that only
// need to seed a known namespace->home_node mapping. defaultHomeNode is the
// HomeNode returned for any name not in byName; setting it lets tests that
// don't care about per-namespace placement reuse this fake without seeding.
type fakeNamespacesExister struct {
	byName          map[string]cmd.Namespace
	defaultHomeNode string
}

func (f fakeNamespacesExister) Exists(name string) bool {
	if _, ok := f.byName[name]; ok {
		return true
	}
	return f.defaultHomeNode != ""
}

func (f fakeNamespacesExister) IsActive(name string) bool {
	if ns, ok := f.byName[name]; ok {
		return ns.State == cmd.NamespaceStateActive
	}
	return f.defaultHomeNode != ""
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
		DefaultVectorizerModule:     config.VectorizerModuleNone,
		DefaultVectorDistanceMetric: "cosine",
	}
	cfg.Namespaces.Enabled = enabled
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, dummyParseVectorConfig, fakeValidator, fakeModulesProvider{}, nil, nil)
	// Default exister returns HomeNode=node-1 for any name, matching the
	// default storage candidate set in fakeSchemaManager.StorageCandidates().
	// Tests that exercise placement should set handler.namespacesExister
	// directly to override this default.
	handler, err := NewHandler(
		schemaManager, schemaManager, fakeValidator, logger, mocks.NewMockAuthorizer(),
		&cfg.SchemaHandlerConfig, cfg, dummyParseVectorConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, nil, *schemaParser, nil,
		fakeNamespacesExister{defaultHomeNode: "node-1"})
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
		name       string
		enabled    bool
		principal  *models.Principal
		class      string
		wantClass  string
		wantErrMsg string
	}{
		{
			name:      "namespaced principal qualifies and persists prefixed class",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			class:     "Movies",
			wantClass: "customer1:Movies",
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

			if tt.wantClass != "" {
				sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
					return c.Class == tt.wantClass
				}), mock.Anything).Return(nil)
				sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil)
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

// TestAddClass_PinsShardsToNamespaceHomeNode covers the placement override
// on NS-enabled clusters: the shard state is built with [home_node] as the
// candidate list, instead of the full storage candidate set. NS-disabled
// clusters fall back to the default-spread behaviour.
func TestAddClass_PinsShardsToNamespaceHomeNode(t *testing.T) {
	t.Parallel()

	allCandidates := []string{"node-1", "node-2", "node-3"}

	tests := []struct {
		name      string
		enabled   bool
		principal *models.Principal
		exister   fakeNamespacesExister
		inputName string
		// allowedNodes is the set the picked shard node is expected to be
		// a member of. With NS enabled and a pinned home_node it's the
		// singleton {home_node}; with NS disabled InitState spreads across
		// all storage candidates and picks one.
		allowedNodes []string
	}{
		{
			name:      "NS enabled: shard pinned to namespace home_node",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister: fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
			}},
			inputName:    "Movies",
			allowedNodes: []string{"node-2"},
		},
		{
			name:         "NS disabled: pick from full storage candidates",
			enabled:      false,
			principal:    globalPrincipal(),
			exister:      fakeNamespacesExister{},
			inputName:    "Movies",
			allowedNodes: allCandidates,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)
			sm.storageCandidates = allCandidates
			handler.namespacesExister = tt.exister

			var capturedState *sharding.State
			sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
				return c != nil
			}), mock.MatchedBy(func(s *sharding.State) bool {
				capturedState = s
				return true
			})).Return(nil)
			sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil)

			class := &models.Class{
				Class:             tt.inputName,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			_, _, err := handler.AddClass(context.Background(), tt.principal, class)
			require.NoError(t, err)

			require.NotNil(t, capturedState, "expected AddClass to be called with a sharding state")
			require.Len(t, capturedState.Physical, 1, "single non-MT shard expected")
			for _, phys := range capturedState.Physical {
				require.Len(t, phys.BelongsToNodes, 1, "RF=1 should pick exactly one node")
				assert.Contains(t, tt.allowedNodes, phys.BelongsToNodes[0])
			}
		})
	}
}

// TestAddClass_NamespacePlacementErrors covers the failure modes that
// surface before the RAFT propose: missing namespace, missing home_node, or
// a home_node that no longer belongs to the cluster's storage candidates.
func TestAddClass_NamespacePlacementErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		exister    fakeNamespacesExister
		candidates []string
		wantErr    string
	}{
		{
			name:       "namespace not found",
			exister:    fakeNamespacesExister{}, // no seed
			candidates: []string{"node-1"},
			wantErr:    "namespace \"customer1\" not found",
		},
		{
			name: "home_node empty",
			exister: fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: nil, State: cmd.NamespaceStateActive},
			}},
			candidates: []string{"node-1"},
			wantErr:    "has no home_node",
		},
		{
			name: "home_node not in storage candidates",
			exister: fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: []string{"node-9"}, State: cmd.NamespaceStateActive},
			}},
			candidates: []string{"node-1"},
			wantErr:    "is not a current storage candidate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, true)
			sm.storageCandidates = tt.candidates
			handler.namespacesExister = tt.exister

			sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil)
			class := &models.Class{
				Class:             "Movies",
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}
			_, _, err := handler.AddClass(context.Background(), namespacedPrincipal("customer1"), class)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
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
			sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil).Maybe()

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
	sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil).Maybe()

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
	sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil)

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
	sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil)

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
	assert.Equal(t, 1, got.DesiredCount, "override caps DesiredCount to len(candidates)")
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
			sm.On("QueryCollectionsCount", mock.Anything).Return(0, nil)

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

// TestAddTenants_PinsShardsToNamespaceHomeNode covers the placement
// override for tenant creation on NS-enabled clusters: the AddTenants RPC
// is built with ClusterNodes=[home_node]. NS-disabled clusters fall back
// to the full storage candidate set.
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
	}{
		{
			name:      "NS enabled: ClusterNodes pinned to home_node",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister: fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
			}},
			class:     "Movies",
			wantNodes: []string{"node-2"},
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
			require.NoError(t, err)
			require.NotNil(t, captured, "expected AddTenants to be called")
			assert.Equal(t, tt.wantNodes, captured.ClusterNodes)
		})
	}
}

// TestUpdateTenants_PinsShardsToNamespaceHomeNode covers the placement
// override on the UpdateTenants path. The candidate list is consumed by
// the apply-side unfreeze flow (GetPartitions), so pinning to [home_node]
// here is what lands a reactivated tenant back on the namespace's home
// node. Non-unfreeze transitions ignore the candidate list, so we just
// assert what's serialized in the propose payload.
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
			name:      "NS enabled: ClusterNodes pinned to home_node",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			exister: fakeNamespacesExister{byName: map[string]cmd.Namespace{
				"customer1": {Name: "customer1", HomeNodes: []string{"node-2"}, State: cmd.NamespaceStateActive},
			}},
			class:     "Movies",
			wantNodes: []string{"node-2"},
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
			sm.On("TenantsShardsWithVersion", mock.Anything, uint64(0), mock.Anything, mock.Anything).
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

// TestUpdateClass_QualifiesPropertyDataTypes pins the GET→PUT round-trip:
// a namespaced caller's stripped cross-ref DataType is qualified before
// reaching the SchemaManager; foreign-prefix entries are rejected.
func TestUpdateClass_QualifiesPropertyDataTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		enabled       bool
		principal     *models.Principal
		storedClass   string
		bodyClass     string
		bodyDataType  []string
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

			stored := &models.Class{
				Class:             tt.storedClass,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				Properties: []*models.Property{
					{Name: "watched", DataType: tt.wantStoredDT},
				},
			}
			sm.On("ReadOnlyClass", tt.storedClass).Return(stored).Maybe()

			var captured *models.Class
			sm.On("UpdateClass", mock.MatchedBy(func(c *models.Class) bool {
				captured = c
				return true
			}), mock.Anything).Return(nil).Maybe()

			body := &models.Class{
				Class:             tt.bodyClass,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				Properties: []*models.Property{
					{Name: "watched", DataType: tt.bodyDataType},
				},
			}

			err := handler.UpdateClass(context.Background(), tt.principal, tt.bodyClass, body)
			if tt.wantErrSubstr != "" {
				require.Error(t, err)
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

// TestAddClass_NamespacedCollectionLimit checks that the
// MAXIMUM_ALLOWED_COLLECTIONS_COUNT cap is enforced per namespace when
// namespaces are enabled, using principal.Namespace as the selector.
//
// The mock matcher on QueryCollectionsCount(<principalNS>) is what proves
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

			sm.On("QueryCollectionsCount", tt.principalNS).Return(tt.existingCount, nil)
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
