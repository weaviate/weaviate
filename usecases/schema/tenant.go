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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// AddTenants is used to add new tenants to a class
// Class must exit and has partitioning enabled
func (m *Manager) AddTenants(ctx context.Context, principal *models.Principal,
	class string, tenants []string,
) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	cls, st := m.getClassByName(class), m.ShardingState(class)
	if cls == nil || st == nil {
		return ErrNotFound
	}
	if !isMultiTenancyEnabled(cls.MultiTenancyConfig) {
		return fmt.Errorf("multi-tenancy is not enabled for class %q", class)
	}
	rf := int64(1)
	if cls.ReplicationConfig != nil && cls.ReplicationConfig.Factor > rf {
		rf = cls.ReplicationConfig.Factor
	}

	for _, name := range tenants {
		if name == "" {
			return fmt.Errorf("found empty tenant key at index %d", rf)
		}
		// TODO: validate p.Name (length, charset, case sensitivity)
	}
	partitions, err := st.GetPartitions(m.clusterState, tenants, rf)
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

	return m.onAddPartitions(ctx, st, request)
}

func (m *Manager) onAddPartitions(ctx context.Context,
	st *sharding.State, request AddPartitionsPayload,
) error {
	for _, p := range request.Partitions {
		if _, ok := st.Physical[p.Name]; !ok {
			st.AddPartition(p.Name, p.Nodes)
		}
	}

	if err := m.saveSchema(ctx); err != nil {
		return err
	}

	m.shardingStateLock.Lock()
	m.state.ShardingState[request.ClassName] = st
	m.shardingStateLock.Unlock()
	shards := make([]string, len(request.Partitions))
	for i, p := range request.Partitions {
		shards[i] = p.Name
	}
	// this should actually not fail but just in case
	// TODO: make sure AddPartitions() never fails
	if err := m.migrator.AddPartitions(ctx, request.ClassName, shards); err != nil {
		m.logger.WithField("action", "add_partitions").
			WithField("class", request.ClassName).Error(err)
	}
	return nil
}

func isMultiTenancyEnabled(cfg *models.MultiTenancyConfig) bool {
	return cfg != nil && cfg.Enabled && cfg.TenantKey != ""
}
