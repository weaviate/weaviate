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
func (m *Manager) AddTenants(ctx context.Context, principal *models.Principal, class string, tenants []*models.Tenant) error {
	if err := m.Authorizer.Authorize(principal, "update", tenantsPath); err != nil {
		return err
	}

	// validation
	if err := validateTenantNames(tenants); err != nil {
		return err
	}
	cls, st := m.getClassByName(class), m.ShardingState(class)
	if cls == nil || st == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !isMultiTenancyEnabled(cls.MultiTenancyConfig) {
		return fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	// create transaction payload
	rf := int64(1)
	if cls.ReplicationConfig != nil && cls.ReplicationConfig.Factor > rf {
		rf = cls.ReplicationConfig.Factor
	}

	tenantNames := make([]string, len(tenants))
	for i, tenant := range tenants {
		tenantNames[i] = tenant.Name
	}
	partitions, err := st.GetPartitions(m.clusterState, tenantNames, rf)
	if err != nil {
		return fmt.Errorf("get partitions from sharding state: %w", err)
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

	return m.onAddTenants(ctx, st, cls, request) // actual update
}

func validateTenantNames(tenants []*models.Tenant) error {
	for i, tenant := range tenants {
		if !regexTenantName.MatchString(tenant.Name) {
			return uco.NewErrInvalidUserInput("invalid tenant name at index %d", i)
		}
	}
	return nil
}

func (m *Manager) onAddTenants(ctx context.Context,
	st *sharding.State, class *models.Class, request AddTenantsPayload,
) error {
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
		if st.IsShardLocal(p.Name) {
			shards = append(shards, p.Name)
		}
	}

	commit, err := m.migrator.NewTenants(ctx, class, shards)
	if err != nil {
		m.logger.WithField("action", "add_tenants").
			WithField("class", request.Class).Error(err)
	}

	st.SetLocalName(m.clusterState.LocalName())

	m.logger.
		WithField("action", "schema.add_tenants").
		Debug("saving updated schema to configuration store")

	if err := m.repo.NewShards(ctx, class.Class, pairs); err != nil {
		commit(false) // rollback adding new tenant
		return err
	}
	commit(true) // commit new adding new tenant
	m.shardingStateLock.Lock()
	m.state.ShardingState[request.Class] = st
	m.shardingStateLock.Unlock()
	m.triggerSchemaUpdateCallbacks()

	return nil
}

// DeleteTenants is used to delete tenants of a class
// Class must exist and has partitioning enabled
func (m *Manager) DeleteTenants(ctx context.Context, principal *models.Principal, class string, tenants []*models.Tenant) error {
	if err := m.Authorizer.Authorize(principal, "delete", tenantsPath); err != nil {
		return err
	}
	// validation
	if err := validateTenantNames(tenants); err != nil {
		return err
	}
	cls := m.getClassByName(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	if !isMultiTenancyEnabled(cls.MultiTenancyConfig) {
		return fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}

	// create transaction payload
	tenantNames := make([]string, len(tenants))
	for i, tenant := range tenants {
		tenantNames[i] = tenant.Name
	}

	request := DeleteTenantsPayload{
		Class:   class,
		Tenants: tenantNames,
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

func isMultiTenancyEnabled(cfg *models.MultiTenancyConfig) bool {
	return cfg != nil && cfg.Enabled
}
