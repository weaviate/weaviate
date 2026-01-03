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
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	validator    validator
	repo         SchemaStore
	logger       logrus.FieldLogger
	Authorizer   authorization.Authorizer
	clusterState clusterState

	sync.RWMutex
	// The handler is responsible for well-defined tasks and should be decoupled from the manager.
	// This enables API requests to be directed straight to the handler without the need to pass through the manager.
	// For more context, refer to the handler's definition.
	Handler

	SchemaReader
}

type VectorConfigParser func(in interface{}, vectorIndexType string, isMultiVector bool) (schemaConfig.VectorIndexConfig, error)

type InvertedConfigValidator func(in *models.InvertedIndexConfig) error

type SchemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
	ReadOnlyClass(string) *models.Class
	ResolveAlias(string) string
	GetAliasesForClass(class string) []*models.Alias
	Nodes() []string
	NodeName() string
	ClusterHealthScore() int
	ResolveParentNodes(string, string) (map[string]string, error)
	Statistics() map[string]any

	ShardOwner(class, shard string) (string, error)
	TenantsShards(ctx context.Context, class string, tenants ...string) (map[string]string, error)
	OptimisticTenantStatus(ctx context.Context, class string, tenants string) (map[string]string, error)
	ShardFromUUID(class string, uuid []byte) string
	ShardReplicas(class, shard string) ([]string, error)
}

type VectorizerValidator interface {
	ValidateVectorizer(moduleName string) error
}

type ModuleConfig interface {
	SetClassDefaults(class *models.Class)
	SetSinglePropertyDefaults(class *models.Class, props ...*models.Property)
	ValidateClass(ctx context.Context, class *models.Class) error
	GetByName(name string) modulecapabilities.Module
	IsGenerative(string) bool
	IsReranker(string) bool
	IsMultiVector(string) bool
}

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ObjectSchema  *models.Schema `json:"object"`
	ShardingState map[string]*sharding.State
}

// NewState returns a new state with room for nClasses classes
func NewState(nClasses int) State {
	return State{
		ObjectSchema: &models.Schema{
			Classes: make([]*models.Class, 0, nClasses),
		},
		ShardingState: make(map[string]*sharding.State, nClasses),
	}
}

func (s State) EqualEnough(other *State) bool {
	// Same number of classes
	eqClassLen := len(s.ObjectSchema.Classes) == len(other.ObjectSchema.Classes)
	if !eqClassLen {
		return false
	}

	// Same sharding state length
	eqSSLen := len(s.ShardingState) == len(other.ShardingState)
	if !eqSSLen {
		return false
	}

	for cls, ss1ss := range s.ShardingState {
		// Same sharding state keys
		ss2ss, ok := other.ShardingState[cls]
		if !ok {
			return false
		}

		// Same number of physical shards
		eqPhysLen := len(ss1ss.Physical) == len(ss2ss.Physical)
		if !eqPhysLen {
			return false
		}

		for shard, ss1phys := range ss1ss.Physical {
			// Same physical shard contents and status
			ss2phys, ok := ss2ss.Physical[shard]
			if !ok {
				return false
			}
			eqActivStat := ss1phys.ActivityStatus() == ss2phys.ActivityStatus()
			if !eqActivStat {
				return false
			}
		}
	}

	return true
}

// SchemaStore is responsible for persisting the schema
// by providing support for both partial and complete schema updates
// Deprecated: instead schema now is persistent via RAFT
// see : usecase/schema/handler.go & cluster/store/store.go
// Load and save are left to support backward compatibility
type SchemaStore interface {
	// Save saves the complete schema to the persistent storage
	Save(ctx context.Context, schema State) error

	// Load loads the complete schema from the persistent storage
	Load(context.Context) (State, error)
}

// KeyValuePair is used to serialize shards updates
type KeyValuePair struct {
	Key   string
	Value []byte
}

// ClassPayload is used to serialize class updates
type ClassPayload struct {
	Name          string
	Metadata      []byte
	ShardingState []byte
	Shards        []KeyValuePair
	ReplaceShards bool
	Error         error
}

type clusterState interface {
	cluster.NodeSelector
	// Hostnames initializes a broadcast
	Hostnames() []string

	// AllNames initializes shard distribution across nodes
	AllNames() []string
	NodeCount() int

	// ClusterHealthScore gets the whole cluster health, the lower number the better
	ClusterHealthScore() int

	SchemaSyncIgnored() bool
	SkipSchemaRepair() bool
}

// NewManager creates a new manager
func NewManager(validator validator,
	schemaManager SchemaManager,
	schemaReader SchemaReader,
	repo SchemaStore,
	logger logrus.FieldLogger, authorizer authorization.Authorizer,
	schemaConfig *config.SchemaHandlerConfig,
	config config.Config,
	configParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	cloud modulecapabilities.OffloadCloud,
	parser Parser,
	collectionRetrievalStrategyFF *configRuntime.FeatureFlag[string],
) (*Manager, error) {
	handler, err := NewHandler(
		schemaReader,
		schemaManager,
		validator,
		logger, authorizer,
		schemaConfig,
		config, configParser, vectorizerValidator, invertedConfigValidator,
		moduleConfig, clusterState, cloud, parser, NewClassGetter(&parser, schemaManager, schemaReader, collectionRetrievalStrategyFF, logger),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot init handler: %w", err)
	}
	m := &Manager{
		validator:    validator,
		repo:         repo,
		logger:       logger,
		clusterState: clusterState,
		Handler:      handler,
		SchemaReader: schemaReader,
		Authorizer:   authorizer,
	}

	return m, nil
}

func (m *Manager) ClusterHealthScore() int {
	return m.clusterState.ClusterHealthScore()
}

// ResolveParentNodes gets all replicas for a specific class shard and resolves their names
//
// it returns map[node_name] node_address where node_address = "" if can't resolve node_name
func (m *Manager) ResolveParentNodes(class, shardName string) (map[string]string, error) {
	nodes, err := m.ShardReplicas(class, shardName)
	if err != nil {
		return nil, fmt.Errorf("get replicas from schema: %w", err)
	}

	if len(nodes) == 0 {
		return nil, nil
	}

	name2Addr := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node != "" {
			host, _ := m.clusterState.NodeHostname(node)
			name2Addr[node] = host
		}
	}
	return name2Addr, nil
}

func (m *Manager) TenantsShards(ctx context.Context, class string, tenants ...string) (map[string]string, error) {
	slices.Sort(tenants)
	tenants = slices.Compact(tenants)
	status, _, err := m.schemaManager.QueryTenantsShards(class, tenants...)
	if !m.AllowImplicitTenantActivation(class) || err != nil {
		return status, err
	}

	return m.activateTenantIfInactive(ctx, class, status)
}

// OptimisticTenantStatus tries to query the local state first. It is
// optimistic that the state has already propagated correctly. If the state is
// unexpected, i.e. either the tenant is not found at all or the status is
// COLD, it will double-check with the leader.
//
// This way we accept false positives (for HOT tenants), but guarantee that there will never be
// false negatives (i.e. tenants labelled as COLD that the leader thinks should
// be HOT).
//
// This means:
//
//   - If a tenant is HOT locally (true positive), we proceed normally
//   - If a tenant is HOT locally, but should be COLD (false positive), we still
//     proceed. This is a conscious decision to keep the happy path free from
//     (expensive) leader lookups.
//   - If a tenant is not found locally, we assume it was recently created, but
//     the state hasn't propagated yet. To verify, we check with the leader.
//   - If a tenant is found locally, but is marked as COLD, we assume it was
//     recently turned HOT, but the state hasn't propagated yet. To verify, we
//     check with the leader
//
// Overall, we keep the (very common) happy path, free from expensive
// leader-lookups and only fall back to the leader if the local result implies
// an unhappy path.
func (m *Manager) OptimisticTenantStatus(ctx context.Context, class string, tenant string) (map[string]string, error) {
	var foundTenant bool
	var status string
	err := m.schemaReader.Read(class, true, func(_ *models.Class, ss *sharding.State) error {
		t, ok := ss.Physical[tenant]
		if !ok {
			return nil
		}

		foundTenant = true
		status = t.Status
		return nil
	})
	if err != nil {
		return nil, err
	}

	if !foundTenant || status != models.TenantActivityStatusHOT {
		// either no state at all or state does not imply happy path -> delegate to
		// leader
		return m.TenantsShards(ctx, class, tenant)
	}

	return map[string]string{
		tenant: status,
	}, nil
}

func (m *Manager) activateTenantIfInactive(ctx context.Context, class string,
	status map[string]string,
) (map[string]string, error) {
	req := &api.UpdateTenantsRequest{
		Tenants:               make([]*api.Tenant, 0, len(status)),
		ClusterNodes:          m.schemaManager.StorageCandidates(),
		ImplicitUpdateRequest: true,
	}
	for tenant, s := range status {
		if s != models.TenantActivityStatusHOT {
			req.Tenants = append(req.Tenants,
				&api.Tenant{Name: tenant, Status: models.TenantActivityStatusHOT})
		}
	}

	if len(req.Tenants) == 0 {
		// nothing to do, all tenants are already HOT
		return status, nil
	}

	_, err := m.schemaManager.UpdateTenants(ctx, class, req)
	if err != nil {
		names := make([]string, len(req.Tenants))
		for i, t := range req.Tenants {
			names[i] = t.Name
		}

		return nil, fmt.Errorf("implicit activation of tenants %s: %w", strings.Join(names, ", "), err)
	}

	for _, t := range req.Tenants {
		status[t.Name] = models.TenantActivityStatusHOT
	}

	return status, nil
}

func (m *Manager) AllowImplicitTenantActivation(class string) bool {
	allow := false
	m.schemaReader.Read(class, true, func(c *models.Class, _ *sharding.State) error {
		allow = schema.AutoTenantActivationEnabled(c)
		return nil
	})

	return allow
}

func (m *Manager) ShardOwner(class, shard string) (string, error) {
	owner, _, err := m.schemaManager.QueryShardOwner(class, shard)
	if err != nil {
		return "", err
	}
	return owner, nil
}
