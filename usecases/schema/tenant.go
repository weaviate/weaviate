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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	uco "github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var regexTenantName = regexp.MustCompile(`[A-Za-z0-9\-]+`)

// AddTenants is used to add new tenants to a class
// Class must exit and has partitioning enabled
func (m *Manager) AddTenants(ctx context.Context, principal *models.Principal, class string, tenants []*models.Tenant) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

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
	rf := int64(1)
	if cls.ReplicationConfig != nil && cls.ReplicationConfig.Factor > rf {
		rf = cls.ReplicationConfig.Factor
	}

	tenantNames := make([]string, len(tenants))
	for i, tenant := range tenants {
		if tenant.Name == "" {
			return fmt.Errorf("found empty tenant key at index %d", rf)
		}
		// TODO: validate p.Name (length, charset, case sensitivity)
		tenantNames[i] = tenant.Name
	}
	partitions, err := st.GetPartitions(m.clusterState, tenantNames, rf)
	if err != nil {
		return fmt.Errorf("get partitions: %w", err)
	}
	request := AddPartitionsPayload{
		ClassName:  class,
		Partitions: make([]Partition, len(partitions)),
	}
	i := 0
	for name, owners := range partitions {
		request.Partitions[i] = Partition{Name: name, Nodes: owners}
		i++
	}

	tx, err := m.cluster.BeginTransaction(ctx, AddPartitions,
		request, DefaultTxTTL)
	if err != nil {
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		m.logger.WithError(err).Errorf("not every node was able to commit")
	}

	return m.onAddPartitions(ctx, st, cls, request)
}

func validateTenantNames(tenants []*models.Tenant) error {
	for _, tenant := range tenants {
		// currently only support alphanumeric or uuid
		valid := regexTenantName.MatchString(tenant.Name)
		if !valid {
			return uco.NewErrInvalidUserInput("invalid tenant name %q", tenant.Name)
		}
	}
	return nil
}

func (m *Manager) onAddPartitions(ctx context.Context,
	st *sharding.State, class *models.Class, request AddPartitionsPayload,
) error {
	pairs := make([]KeyValuePair, 0, len(request.Partitions))
	for _, p := range request.Partitions {
		if _, ok := st.Physical[p.Name]; !ok {
			p := st.AddPartition(p.Name, p.Nodes)
			data, err := json.Marshal(p)
			if err != nil {
				return fmt.Errorf("cannot marshal partition %s: %w", p.Name, err)
			}
			pairs = append(pairs, KeyValuePair{p.Name, data})
		}
	}
	shards := make([]string, 0, len(request.Partitions))
	for _, p := range request.Partitions {
		if st.IsShardLocal(p.Name) {
			shards = append(shards, p.Name)
		}
	}

	commit, err := m.migrator.NewPartitions(ctx, class, shards)
	if err != nil {
		m.logger.WithField("action", "add_partitions").
			WithField("class", request.ClassName).Error(err)
	}

	st.SetLocalName(m.clusterState.LocalName())

	m.logger.
		WithField("action", "schema.add_tenants").
		Debug("saving updated schema to configuration store")

	if err := m.repo.NewShards(ctx, class.Class, pairs); err != nil {
		commit(false) // rollback new partitions
		return err
	}
	commit(true) // commit new partitions
	m.shardingStateLock.Lock()
	m.state.ShardingState[request.ClassName] = st
	m.shardingStateLock.Unlock()
	m.triggerSchemaUpdateCallbacks()

	return nil
}

func isMultiTenancyEnabled(cfg *models.MultiTenancyConfig) bool {
	return cfg != nil && cfg.Enabled && cfg.TenantKey != ""
}
