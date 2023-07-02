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
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	uco "github.com/weaviate/weaviate/usecases/objects"
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
) (err error) {
	if err := m.Authorizer.Authorize(principal, "update", tenantsPath); err != nil {
		return err
	}
	tenantNames := make([]string, len(tenants))
	for i, tenant := range tenants {
		tenantNames[i] = tenant.Name
	}

	// validation
	if err := validateTenants(tenantNames); err != nil {
		return err
	}
	cls := m.getClassByName(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !schema.MultiTenancyEnabled(cls) {
		return fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	// create transaction payload
	partitions, err := m.getPartitions(cls, tenantNames)
	if err != nil {
		return fmt.Errorf("get partitions from class %q: %w", class, err)
	}
	request := AddTenantsPayload{
		Class:   class,
		Tenants: make([]Tenant, len(partitions)),
	}
	i := 0
	for name, owners := range partitions {
		request.Tenants[i] = Tenant{Name: name, Nodes: owners}
		i++
	}

	// open cluster-wide transaction
	tx, err := m.cluster.BeginTransaction(ctx, addTenants,
		request, DefaultTxTTL)
	if err != nil {
		return fmt.Errorf("open cluster-wide transaction: %w", err)
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

	return err
}

func (m *Manager) getPartitions(cls *models.Class, shards []string) (map[string][]string, error) {
	rf := int64(1)
	if cls.ReplicationConfig != nil && cls.ReplicationConfig.Factor > rf {
		rf = cls.ReplicationConfig.Factor
	}
	m.shardingStateLock.RLock()
	defer m.shardingStateLock.RUnlock()
	st := m.state.ShardingState[cls.Class]
	if st == nil {
		return nil, fmt.Errorf("sharding state %w", ErrNotFound)
	}
	return st.GetPartitions(m.clusterState, shards, rf)
}

func validateTenants(tenants []string) error {
	names := make(map[string]struct{}, len(tenants)) // check for name uniqueness
	for i, tenant := range tenants {
		_, nameExists := names[tenant]
		if nameExists {
			return uco.NewErrInvalidUserInput("duplicate tenant name %s", tenant)
		}
		names[tenant] = struct{}{}

		if !regexTenantName.MatchString(tenant) {
			return uco.NewErrInvalidUserInput("invalid tenant name at index %d", i)
		}
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
			p := st.AddPartition(p.Name, p.Nodes)
			data, err := json.Marshal(p)
			if err != nil {
				return fmt.Errorf("cannot marshal partition %s: %w", p.Name, err)
			}
			pairs = append(pairs, KeyValuePair{p.Name, data})
		}
	}
	shards := make([]string, 0, len(request.Tenants))
	for _, p := range request.Tenants {
		if st.IsLocalShard(p.Name) {
			shards = append(shards, p.Name)
		}
	}

	commit, err := m.migrator.NewTenants(ctx, class, shards)
	if err != nil {
		return fmt.Errorf("migrator.new_tenants: %w", err)
	}

	m.logger.
		WithField("action", "schema.add_tenants").
		Debug("saving updated schema to configuration store")

	if err := m.repo.NewShards(ctx, class.Class, pairs); err != nil {
		commit(false) // rollback adding new tenant
		return err
	}
	commit(true) // commit new adding new tenant
	m.shardingStateLock.Lock()
	ost := m.state.ShardingState[request.Class]
	for name, p := range st.Physical {
		ost.Physical[name] = p
	}
	m.shardingStateLock.Unlock()
	m.triggerSchemaUpdateCallbacks()

	return nil
}

// DeleteTenants is used to delete tenants of a class.
//
// Class must exist and has partitioning enabled
func (m *Manager) DeleteTenants(ctx context.Context, principal *models.Principal, class string, tenants []string) error {
	if err := m.Authorizer.Authorize(principal, "delete", tenantsPath); err != nil {
		return err
	}
	// validation
	if err := validateTenants(tenants); err != nil {
		return err
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
	m.shardingStateLock.Lock()
	if ss := m.state.ShardingState[req.Class]; ss != nil {
		for _, p := range req.Tenants {
			ss.DeletePartition(p)
		}
	}
	m.shardingStateLock.Unlock()
	m.triggerSchemaUpdateCallbacks()

	return nil
}
