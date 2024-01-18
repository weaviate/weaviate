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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	uco "github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var regexTenantName = regexp.MustCompile(`^` + schema.ShardNameRegexCore + `$`)

// tenantsPath is the main path used for authorization
const tenantsPath = "schema/tenants"

// AddTenants is used to add new tenants to a class
// Class must exist and has partitioning enabled
func (m *Manager) AddTenants(ctx context.Context,
	principal *models.Principal,
	class string,
	tenants []*models.Tenant,
) (created []*models.Tenant, err error) {
	if err = m.Authorizer.Authorize(principal, "update", tenantsPath); err != nil {
		return
	}

	validated, err := validateTenants(tenants)
	if err != nil {
		return
	}
	if err = validateActivityStatuses(validated, true); err != nil {
		return
	}
	cls := m.getClassByName(class)
	if cls == nil {
		err = fmt.Errorf("class %q: %w", class, ErrNotFound)
		return
	}
	if !schema.MultiTenancyEnabled(cls) {
		err = fmt.Errorf("multi-tenancy is not enabled for class %q", class)
		return
	}

	names := make([]string, len(validated))
	for i, tenant := range validated {
		names[i] = tenant.Name
	}

	// create transaction payload
	partitions, err := m.getPartitions(cls, names)
	if err != nil {
		err = fmt.Errorf("get partitions from class %q: %w", class, err)
		return
	}
	if len(partitions) != len(names) {
		m.logger.WithField("action", "add_tenants").
			WithField("#partitions", len(partitions)).
			WithField("#requested", len(names)).
			Tracef("number of partitions for class %q does not match number of requested tenants", class)
	}
	request := AddTenantsPayload{
		Class:   class,
		Tenants: make([]TenantCreate, 0, len(partitions)),
	}
	for i, name := range names {
		part, ok := partitions[name]
		if ok {
			request.Tenants = append(request.Tenants, TenantCreate{
				Name:   name,
				Nodes:  part,
				Status: schema.ActivityStatus(validated[i].ActivityStatus),
			})
		}
	}

	// open cluster-wide transaction
	tx, err := m.cluster.BeginTransaction(ctx, addTenants,
		request, DefaultTxTTL)
	if err != nil {
		err = fmt.Errorf("open cluster-wide transaction: %w", err)
		return
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		m.logger.WithError(err).Errorf("not every node was able to commit")
	}

	err = m.onAddTenants(ctx, cls, request) // actual update
	if err != nil {
		m.logger.WithField("action", "add_tenants").
			WithField("n", len(request.Tenants)).
			WithField("class", cls.Class).Error(err)
	}

	created = validated
	return
}

func (m *Manager) getPartitions(cls *models.Class, shards []string) (map[string][]string, error) {
	rf := int64(1)
	if cls.ReplicationConfig != nil && cls.ReplicationConfig.Factor > rf {
		rf = cls.ReplicationConfig.Factor
	}
	m.schemaCache.RLock()
	defer m.schemaCache.RUnlock()
	st := m.schemaCache.ShardingState[cls.Class]
	if st == nil {
		return nil, fmt.Errorf("sharding state %w", ErrNotFound)
	}
	return st.GetPartitions(m.clusterState, shards, rf)
}

func validateTenants(tenants []*models.Tenant) (validated []*models.Tenant, err error) {
	uniq := make(map[string]*models.Tenant)
	for i, requested := range tenants {
		if !regexTenantName.MatchString(requested.Name) {
			msg := "tenant name should only contain alphanumeric characters (a-z, A-Z, 0-9), " +
				"underscore (_), and hyphen (-), with a length between 1 and 64 characters"
			err = uco.NewErrInvalidUserInput("tenant name at index %d: %s", i, msg)
			return
		}
		_, found := uniq[requested.Name]
		if !found {
			uniq[requested.Name] = requested
		}
	}
	validated = make([]*models.Tenant, len(uniq))
	i := 0
	for _, tenant := range uniq {
		validated[i] = tenant
		i++
	}
	return
}

func validateActivityStatuses(tenants []*models.Tenant, allowEmpty bool) error {
	msgs := make([]string, 0, len(tenants))

	for _, tenant := range tenants {
		switch status := tenant.ActivityStatus; status {
		case models.TenantActivityStatusHOT, models.TenantActivityStatusCOLD:
			// ok
		case models.TenantActivityStatusWARM, models.TenantActivityStatusFROZEN:
			msgs = append(msgs, fmt.Sprintf(
				"not yet supported activity status '%s' for tenant %q", status, tenant.Name))
		default:
			if status == "" && allowEmpty {
				continue
			}
			msgs = append(msgs, fmt.Sprintf(
				"invalid activity status '%s' for tenant %q", status, tenant.Name))
		}
	}

	if len(msgs) != 0 {
		return uco.NewErrInvalidUserInput(strings.Join(msgs, ", "))
	}
	return nil
}

func (m *Manager) onAddTenants(ctx context.Context, class *models.Class, request AddTenantsPayload,
) error {
	st := sharding.State{
		Physical: make(map[string]sharding.Physical, len(request.Tenants)),
	}
	st.SetLocalName(m.clusterState.LocalName())
	pairs := make([]KeyValuePair, 0, len(request.Tenants))
	for _, p := range request.Tenants {
		if _, ok := st.Physical[p.Name]; !ok {
			p := st.AddPartition(p.Name, p.Nodes, p.Status)
			data, err := json.Marshal(p)
			if err != nil {
				return fmt.Errorf("cannot marshal partition %s: %w", p.Name, err)
			}
			pairs = append(pairs, KeyValuePair{p.Name, data})
		}
	}
	creates := make([]*migrate.CreateTenantPayload, 0, len(request.Tenants))
	for _, p := range request.Tenants {
		if st.IsLocalShard(p.Name) {
			creates = append(creates, &migrate.CreateTenantPayload{
				Name:   p.Name,
				Status: p.Status,
			})
		}
	}

	commit, err := m.migrator.NewTenants(ctx, class, creates)
	if err != nil {
		return fmt.Errorf("migrator.new_tenants: %w", err)
	}

	m.logger.
		WithField("action", "schema.add_tenants").
		Debug("saving updated schema to configuration store")

	if err = m.repo.NewShards(ctx, class.Class, pairs); err != nil {
		commit(false) // rollback adding new tenant
		return err
	}
	commit(true) // commit new adding new tenant
	m.schemaCache.LockGuard(func() {
		ost := m.schemaCache.ShardingState[request.Class]
		for name, p := range st.Physical {
			if ost.Physical == nil {
				m.schemaCache.ShardingState[request.Class].Physical = make(map[string]sharding.Physical)
			}
			ost.Physical[name] = p
		}
	})

	return nil
}

// UpdateTenants is used to set activity status of tenants of a class.
//
// Class must exist and has partitioning enabled
func (m *Manager) UpdateTenants(ctx context.Context, principal *models.Principal,
	class string, tenants []*models.Tenant,
) error {
	if err := m.Authorizer.Authorize(principal, "update", tenantsPath); err != nil {
		return err
	}
	validated, err := validateTenants(tenants)
	if err != nil {
		return err
	}
	if err := validateActivityStatuses(validated, false); err != nil {
		return err
	}
	cls := m.getClassByName(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !schema.MultiTenancyEnabled(cls) {
		return fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	request := UpdateTenantsPayload{
		Class:   class,
		Tenants: make([]TenantUpdate, len(tenants)),
	}
	for i, tenant := range tenants {
		request.Tenants[i] = TenantUpdate{Name: tenant.Name, Status: tenant.ActivityStatus}
	}

	// open cluster-wide transaction
	tx, err := m.cluster.BeginTransaction(ctx, updateTenants,
		request, DefaultTxTTL)
	if err != nil {
		return fmt.Errorf("open cluster-wide transaction: %w", err)
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		m.logger.WithError(err).Errorf("not every node was able to commit")
	}

	return m.onUpdateTenants(ctx, cls, request) // actual update
}

func (m *Manager) onUpdateTenants(ctx context.Context, class *models.Class, request UpdateTenantsPayload,
) error {
	ssCopy := sharding.State{Physical: make(map[string]sharding.Physical)}
	ssCopy.SetLocalName(m.clusterState.LocalName())

	if err := m.schemaCache.RLockGuard(func() error {
		ss, ok := m.schemaCache.ShardingState[class.Class]
		if !ok {
			return fmt.Errorf("sharding state for class '%s' not found", class.Class)
		}
		for _, tu := range request.Tenants {
			physical, ok := ss.Physical[tu.Name]
			if !ok {
				return fmt.Errorf("tenant '%s' not found", tu.Name)
			}
			// skip if status does not change
			if physical.ActivityStatus() == tu.Status {
				continue
			}
			ssCopy.Physical[tu.Name] = physical.DeepCopy()
		}
		return nil
	}); err != nil {
		return err
	}

	schemaUpdates := make([]KeyValuePair, 0, len(ssCopy.Physical))
	migratorUpdates := make([]*migrate.UpdateTenantPayload, 0, len(ssCopy.Physical))
	for _, tu := range request.Tenants {
		physical, ok := ssCopy.Physical[tu.Name]
		if !ok { // not present due to status not changed, skip
			continue
		}

		physical.Status = tu.Status
		ssCopy.Physical[tu.Name] = physical
		data, err := json.Marshal(physical)
		if err != nil {
			return fmt.Errorf("cannot marshal shard %s: %w", tu.Name, err)
		}
		schemaUpdates = append(schemaUpdates, KeyValuePair{tu.Name, data})

		// skip if not local
		if ssCopy.IsLocalShard(tu.Name) {
			migratorUpdates = append(migratorUpdates, &migrate.UpdateTenantPayload{
				Name:   tu.Name,
				Status: tu.Status,
			})
		}
	}

	commit, err := m.migrator.UpdateTenants(ctx, class, migratorUpdates)
	if err != nil {
		m.logger.WithField("action", "update_tenants").
			WithField("class", request.Class).Error(err)
	}

	m.logger.
		WithField("action", "schema.update_tenants").
		WithField("n", len(request.Tenants)).Debugf("persist schema updates")

	if err := m.repo.UpdateShards(ctx, class.Class, schemaUpdates); err != nil {
		commit(false) // rollback update of tenants
		return err
	}
	commit(true) // commit update of tenants

	// update cache
	m.schemaCache.LockGuard(func() {
		if ss := m.schemaCache.ShardingState[request.Class]; ss != nil {
			for name, physical := range ssCopy.Physical {
				ss.Physical[name] = physical
			}
		}
	})

	return nil
}

// DeleteTenants is used to delete tenants of a class.
//
// Class must exist and has partitioning enabled
func (m *Manager) DeleteTenants(ctx context.Context, principal *models.Principal, class string, tenants []string) error {
	if err := m.Authorizer.Authorize(principal, "delete", tenantsPath); err != nil {
		return err
	}
	for i, name := range tenants {
		if name == "" {
			return fmt.Errorf("empty tenant name at index %d", i)
		}
	}
	cls := m.getClassByName(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !schema.MultiTenancyEnabled(cls) {
		return fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	request := DeleteTenantsPayload{
		Class:   class,
		Tenants: tenants,
	}

	// open cluster-wide transaction
	tx, err := m.cluster.BeginTransaction(ctx, deleteTenants,
		request, DefaultTxTTL)
	if err != nil {
		return fmt.Errorf("open cluster-wide transaction: %w", err)
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		m.logger.WithError(err).Errorf("not every node was able to commit")
	}

	return m.onDeleteTenants(ctx, cls, request) // actual update
}

func (m *Manager) onDeleteTenants(ctx context.Context, class *models.Class, req DeleteTenantsPayload,
) error {
	commit, err := m.migrator.DeleteTenants(ctx, class, req.Tenants)
	if err != nil {
		m.logger.WithField("action", "delete_tenants").
			WithField("class", req.Class).Error(err)
	}

	m.logger.
		WithField("action", "schema.delete_tenants").
		WithField("n", len(req.Tenants)).Debugf("persist schema updates")

	if err := m.repo.DeleteShards(ctx, class.Class, req.Tenants); err != nil {
		commit(false) // rollback deletion of tenants
		return err
	}
	commit(true) // commit deletion of tenants

	// update cache
	m.schemaCache.LockGuard(func() {
		if ss := m.schemaCache.ShardingState[req.Class]; ss != nil {
			for _, p := range req.Tenants {
				ss.DeletePartition(p)
			}
		}
	})

	return nil
}

// GetTenants is used to get tenants of a class.
//
// Class must exist and has partitioning enabled
func (m *Manager) GetTenants(ctx context.Context, principal *models.Principal, class string) ([]*models.Tenant, error) {
	if err := m.Authorizer.Authorize(principal, "get", tenantsPath); err != nil {
		return nil, err
	}
	// validation
	cls := m.getClassByName(class)
	if cls == nil {
		return nil, fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !schema.MultiTenancyEnabled(cls) {
		return nil, fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	var tenants []*models.Tenant
	m.schemaCache.RLockGuard(func() error {
		if ss := m.schemaCache.ShardingState[cls.Class]; ss != nil {
			tenants = make([]*models.Tenant, len(ss.Physical))
			i := 0
			for tenant := range ss.Physical {
				tenants[i] = &models.Tenant{
					Name:           tenant,
					ActivityStatus: schema.ActivityStatus(ss.Physical[tenant].Status),
				}
				i++
			}
		}
		return nil
	})

	return tenants, nil
}
