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

package db

import (
	"context"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// shardMigrations is one shard's side of migration-record reconciliation. A
// shard detached from its index answers with each dependency's own safe
// default rather than failing: no readable local task list, and a seal nobody
// is contending.
//
// Shard.migrationReconciler wires neither node-level answer in this PR: it
// leaves LocalTasks nil and grants every seal, so every cluster verdict
// leaves records standing until the cutover wires them.
type shardMigrations struct {
	shard *Shard
}

func (s *Shard) migrations() shardMigrations { return shardMigrations{shard: s} }

func (m shardMigrations) ReconcileWithClusterTasks(ctx context.Context, tasks []*distributedtask.Task) {
	if m.shard.migrationRecords == nil {
		return
	}
	m.liveReconciler().ReconcileWithClusterTasks(ctx, tasks)
}

func (m shardMigrations) RetireSuperseded(ctx context.Context) {
	if m.shard.migrationRecords == nil {
		return
	}
	m.liveReconciler().RetireSuperseded(ctx)
}

func (m shardMigrations) liveReconciler() *migrationReconciler {
	className := m.shard.index.Config.ClassName.String()
	return m.shard.migrationReconciler(func() *models.Class {
		return m.shard.index.getSchema.ReadOnlyClass(className)
	})
}

func (m shardMigrations) LocalTasks() ([]*distributedtask.Task, bool) {
	if m.shard.index == nil || m.shard.index.db == nil {
		return nil, false
	}
	return m.shard.index.db.migrationCluster.LocalTasks()
}

func (m shardMigrations) SealUnit(desc distributedtask.TaskDescriptor, unitID string) (func(), bool) {
	if m.shard.index == nil || m.shard.index.db == nil {
		return func() {}, true
	}
	return m.shard.index.db.migrationSeals.SealUnit(desc, unitID)
}
