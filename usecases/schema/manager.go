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
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/schema/leader"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/namespaces"
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

	local.SchemaReader
	leaderSchemaReader
}

// leaderSchemaReader lets Manager embed leader.SchemaReader next to
// local.SchemaReader, whose embedded field would otherwise have the same name.
type leaderSchemaReader = leader.SchemaReader

// Manager serves the local and leader schema reads itself; its own methods must
// not shadow them.
var _ Schema = (*Manager)(nil)

// Schema is the use-case layer's schema: the local and leader reads plus tenant
// activation, all served by *Manager. Depend on the narrowest part that covers the
// caller: a local or leader reader, or TenantActivator.
type Schema interface {
	local.SchemaReader
	leader.SchemaReader
	TenantActivator
}

// TenantActivator reads tenant status on behalf of requests and changes tenant
// activity. Unlike leader.TenantReader it may activate tenants: with auto tenant
// activation enabled on the class, a tenant that is not HOT is made HOT, which is a
// RAFT write.
type TenantActivator interface {
	// OptimisticTenantStatus reads the local state first and only asks the leader when
	// the local answer does not show the tenant HOT. allowImplicitActivation may only be
	// set by callers acting for an external user request, because it lets the lookup
	// activate a COLD tenant under auto tenant activation.
	OptimisticTenantStatus(ctx context.Context, class string, tenant string, allowImplicitActivation bool) (map[string]string, error)
	// TenantsShardsStatusWithActivation asks the leader for the tenants' status, activating
	// any that are not HOT under auto tenant activation. It returns the schema version
	// of that activation; writers must pass it to WaitForUpdate before proceeding.
	TenantsShardsStatusWithActivation(ctx context.Context, class string, tenants ...string) (map[string]string, uint64, error)
	DeactivateTenants(ctx context.Context, class string, tenants ...string) error
}

// NewManager creates a new manager
func NewManager(validator validator,
	schemaManager leader.Schema,
	membership cluster.RaftMembership,
	schemaReader local.SchemaReader,
	indexer clusterSchema.Indexer,
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
	namespacesExister namespaces.Exister,
	dropVectorEnqueuer DropVectorIndexEnqueuer,
) (*Manager, error) {
	handler, err := NewHandler(
		schemaReader,
		schemaManager,
		membership,
		indexer,
		validator,
		logger, authorizer,
		schemaConfig,
		config, configParser, vectorizerValidator, invertedConfigValidator,
		moduleConfig, clusterState, cloud, parser, NewClassGetter(&parser, schemaManager, schemaReader, collectionRetrievalStrategyFF, logger),
		namespacesExister,
		dropVectorEnqueuer,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot init handler: %w", err)
	}
	m := &Manager{
		validator:          validator,
		repo:               repo,
		logger:             logger,
		clusterState:       clusterState,
		Handler:            handler,
		SchemaReader:       schemaReader,
		leaderSchemaReader: schemaManager,
		Authorizer:         authorizer,
	}

	return m, nil
}

// TenantsShardsStatusWithActivation asks the RAFT leader for the tenants' status and, when the class
// has auto tenant activation enabled, activates every tenant that is not HOT (a RAFT write).
// It returns the schema version of that activation, 0 if none was needed.
// Callers performing writes should use the returned schemaVersion in WaitForUpdate before proceeding.
func (m *Manager) TenantsShardsStatusWithActivation(ctx context.Context, class string, tenants ...string) (map[string]string, uint64, error) {
	slices.Sort(tenants)
	tenants = slices.Compact(tenants)
	status, version, err := m.schemaManager.TenantsShardsFromLeader(class, tenants...)
	if !m.AllowImplicitTenantActivation(class) || err != nil {
		return status, version, err
	}

	return m.activateTenantIfInactive(ctx, class, status)
}

// OptimisticTenantStatus tries to query the local state first
// allowImplicitActivation may only be set by callers acting for an external user request;
// because it lets the lookup activate a COLD tenant under auto tenant activation and leader lookup.
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
func (m *Manager) OptimisticTenantStatus(ctx context.Context, class string, tenant string,
	allowImplicitActivation bool,
) (map[string]string, error) {
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

	if foundTenant && status == models.TenantActivityStatusHOT {
		return map[string]string{tenant: status}, nil
	}

	// No state at all, or state does not imply the happy path.
	if !allowImplicitActivation {
		if !foundTenant {
			// An empty map reads as "tenant not found" to the caller.
			return map[string]string{}, nil
		}
		return map[string]string{tenant: status}, nil
	}

	statuses, _, err := m.TenantsShardsStatusWithActivation(ctx, class, tenant)
	return statuses, err
}

func (m *Manager) activateTenantIfInactive(ctx context.Context, class string,
	status map[string]string,
) (map[string]string, uint64, error) {
	req := &api.UpdateTenantsRequest{
		Tenants:               make([]*api.Tenant, 0, len(status)),
		ClusterNodes:          m.membership.StorageCandidates(),
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
		return status, 0, nil
	}

	schemaVersion, err := m.schemaManager.UpdateTenants(ctx, class, req)
	if err != nil {
		names := make([]string, len(req.Tenants))
		for i, t := range req.Tenants {
			names[i] = t.Name
		}

		return nil, 0, fmt.Errorf("implicit activation of tenants %s: %w", strings.Join(names, ", "), err)
	}

	for _, t := range req.Tenants {
		status[t.Name] = models.TenantActivityStatusHOT
	}

	return status, schemaVersion, nil
}

func (m *Manager) AllowImplicitTenantActivation(class string) bool {
	allow := false
	m.schemaReader.Read(class, true, func(c *models.Class, _ *sharding.State) error {
		allow = schema.AutoTenantActivationEnabled(c)
		return nil
	})

	return allow
}

func (m *Manager) ActivateTenants(ctx context.Context, class string, tenants ...string) error {
	return m.changeTenantsActivityStatus(ctx, class, tenants, models.TenantActivityStatusHOT)
}

func (m *Manager) DeactivateTenants(ctx context.Context, class string, tenants ...string) error {
	return m.changeTenantsActivityStatus(ctx, class, tenants, models.TenantActivityStatusCOLD)
}

func (m *Manager) changeTenantsActivityStatus(ctx context.Context, class string, tenants []string, status string) error {
	switch ln := len(tenants); ln {
	case 0:
		return nil
	case 1:
		// proceed
	default:
		slices.Sort(tenants)
		tenants = slices.Compact(tenants)
	}

	req := &api.UpdateTenantsRequest{
		Tenants:               make([]*api.Tenant, len(tenants)),
		ClusterNodes:          m.membership.StorageCandidates(),
		ImplicitUpdateRequest: true,
	}
	for i := range tenants {
		req.Tenants[i] = &api.Tenant{Name: tenants[i], Status: status}
	}

	if _, err := m.schemaManager.UpdateTenants(ctx, class, req); err != nil {
		return fmt.Errorf("change tenants %s status to %s: %w", tenants, status, err)
	}
	return nil
}

// EnsureTenantActiveForWrite activates COLD tenants when AutoTenantActivation is enabled.
// Returns the schema version from activation. callers must pass this to WaitForUpdate
func (m *Manager) EnsureTenantActiveForWrite(ctx context.Context, class string, tenants ...string) (uint64, error) {
	_, schemaVersion, err := m.TenantsShardsStatusWithActivation(ctx, class, tenants...)
	return schemaVersion, err
}
