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

package db

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// IdempotentMigrator is used to handle the
// Migrator requests in idempotent nature.
type IdempotentMigrator struct {
	Migrator
}

func NewIdempotentMigrator(db *DB, logger logrus.FieldLogger) *IdempotentMigrator {
	return &IdempotentMigrator{Migrator: Migrator{db, logger}}
}

func (m *IdempotentMigrator) AddClass(ctx context.Context, class *models.Class,
	shardState *sharding.State,
) error {

	// TODO handle idempotence
	m.Migrator.AddClass(ctx, class, shardState)

	return nil
}

func (m *IdempotentMigrator) DropClass(ctx context.Context, class string) error {
	// TODO handle idempotence
	m.Migrator.DropClass(ctx, class)
	return nil
}

func (m *IdempotentMigrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	// TODO handle idempotence
	m.Migrator.UpdateClass(ctx, className, newClassName)
	return nil
}

func (m *IdempotentMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	// TODO handle idempotence
	m.Migrator.AddProperty(ctx, className, prop)
	return nil
}

// DropProperty is ignored, API compliant change
func (m *IdempotentMigrator) DropProperty(ctx context.Context, className string, propertyName string) error {
	// TODO handle idempotence
	return nil
}

func (m *IdempotentMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	// TODO handle idempotence
	return nil
}

func (m *IdempotentMigrator) GetShardsQueueSize(ctx context.Context, className, tenant string) (map[string]int64, error) {
	// TODO handle idempotence
	return m.Migrator.GetShardsQueueSize(ctx, className, tenant)
}

func (m *IdempotentMigrator) GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error) {
	// TODO handle idempotence
	return m.Migrator.GetShardsStatus(ctx, className, tenant)
}

func (m *IdempotentMigrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	// TODO handle idempotence
	return m.Migrator.UpdateShardStatus(ctx, className, shardName, targetStatus)
}

// NewTenants creates new partitions and returns a commit func
// that can be used to either commit or rollback the partitions
func (m *IdempotentMigrator) NewTenants(ctx context.Context, class *models.Class, creates []*migrate.CreateTenantPayload) (commit func(success bool), err error) {
	// TODO handle idempotence
	return m.Migrator.NewTenants(ctx, class, creates)
}

// UpdateTenans activates or deactivates tenant partitions and returns a commit func
// that can be used to either commit or rollback the changes
func (m *IdempotentMigrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*migrate.UpdateTenantPayload) (commit func(success bool), err error) {
	// TODO handle idempotence
	return m.Migrator.UpdateTenants(ctx, class, updates)
}

// DeleteTenants deletes tenants and returns a commit func
// that can be used to either commit or rollback deletion
func (m *IdempotentMigrator) DeleteTenants(ctx context.Context, class *models.Class, tenants []string) (commit func(success bool), err error) {
	// TODO handle idempotence
	return m.Migrator.DeleteTenants(ctx, class, tenants)
}

func (m *IdempotentMigrator) UpdateVectorIndexConfig(ctx context.Context,
	className string, updated schema.VectorIndexConfig,
) error {
	// TODO handle idempotence
	return m.Migrator.UpdateVectorIndexConfig(ctx, className, updated)
}

func (m *IdempotentMigrator) UpdateVectorIndexConfigs(ctx context.Context,
	className string, updated map[string]schema.VectorIndexConfig,
) error {
	// TODO handle idempotence
	return m.Migrator.UpdateVectorIndexConfigs(ctx, className, updated)
}
