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
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func (m *Manager) UpdateClass(ctx context.Context, principal *models.Principal,
	className string, updated *models.Class,
) error {
	m.Lock()
	defer m.Unlock()

	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	initial := m.getClassByName(className)
	if initial == nil {
		return ErrNotFound
	}
	mtEnabled, err := validateUpdatingMT(initial, updated)
	if err != nil {
		return err
	}

	// make sure unset optionals on 'updated' don't lead to an error, as all
	// optionals would have been set with defaults on the initial already
	m.setClassDefaults(updated)

	if err := m.validateImmutableFields(initial, updated); err != nil {
		return err
	}

	if err := m.parseVectorIndexConfig(ctx, updated); err != nil {
		return err
	}

	if err := m.parseShardingConfig(ctx, updated); err != nil {
		return err
	}

	if err := m.migrator.ValidateVectorIndexConfigUpdate(ctx,
		initial.VectorIndexConfig.(schema.VectorIndexConfig),
		updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
		return errors.Wrap(err, "vector index config")
	}

	if err := m.migrator.ValidateInvertedIndexConfigUpdate(ctx,
		initial.InvertedIndexConfig, updated.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}
	if err := validateShardingConfig(initial, updated, mtEnabled, m.clusterState); err != nil {
		return fmt.Errorf("validate sharding config: %w", err)
	}

	if err := replica.ValidateConfigUpdate(initial, updated, m.clusterState); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}

	updatedSharding := updated.ShardingConfig.(sharding.Config)
	initialRF := initial.ReplicationConfig.Factor
	updatedRF := updated.ReplicationConfig.Factor
	var updatedState *sharding.State
	if initialRF != updatedRF {
		uss, err := m.scaleOut.Scale(ctx, className, updatedSharding, initialRF, updatedRF)
		if err != nil {
			return errors.Wrapf(err, "scale out from %d to %d replicas",
				initialRF, updatedRF)
		}
		updatedState = uss
	}

	tx, err := m.cluster.BeginTransaction(ctx, UpdateClass,
		UpdateClassPayload{className, updated, updatedState}, DefaultTxTTL)
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitWriteTransaction(ctx, tx); err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return m.updateClassApplyChanges(ctx, className, updated, updatedState)
}

// validateUpdatingMT validates toggling MT and returns whether mt is enabled
func validateUpdatingMT(current, update *models.Class) (enabled bool, err error) {
	enabled = schema.MultiTenancyEnabled(current)
	if schema.MultiTenancyEnabled(update) != enabled {
		if enabled {
			err = fmt.Errorf("disabling multi-tenancy for an existing class is not supported")
		} else {
			err = fmt.Errorf("enabling multi-tenancy for an existing class is not supported")
		}
	}
	return
}

func validateShardingConfig(current, update *models.Class, mtEnabled bool, cl clusterState) error {
	if mtEnabled {
		return nil
	}
	first, ok := current.ShardingConfig.(sharding.Config)
	if !ok {
		return fmt.Errorf("current config is not well-formed")
	}
	second, ok := update.ShardingConfig.(sharding.Config)
	if !ok {
		return fmt.Errorf("updated config is not well-formed")
	}
	if err := sharding.ValidateConfigUpdate(first, second, cl); err != nil {
		return err
	}
	return nil
}

func (m *Manager) updateClassApplyChanges(ctx context.Context, className string,
	updated *models.Class, updatedShardingState *sharding.State,
) error {
	if updatedShardingState != nil {
		// the sharding state caches the node name, we must therefore set this
		// explicitly now.
		updatedShardingState.SetLocalName(m.clusterState.LocalName())
	}
	if err := m.migrator.UpdateVectorIndexConfig(ctx,
		className, updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
		return errors.Wrap(err, "vector index config")
	}

	if err := m.migrator.UpdateInvertedIndexConfig(ctx, className,
		updated.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}

	if !m.schemaCache.classExist(className) {
		return ErrNotFound
	}

	payload, err := CreateClassPayload(updated, updatedShardingState)
	if err != nil {
		return err
	}
	payload.ReplaceShards = updatedShardingState != nil
	// can be improved by updating the diff

	m.schemaCache.updateClass(updated, updatedShardingState)
	m.logger.
		WithField("action", "schema.update_class").
		Debug("saving updated schema to configuration store")
	// payload.Shards
	if err := m.repo.UpdateClass(ctx, payload); err != nil {
		return err
	}
	m.triggerSchemaUpdateCallbacks()

	return nil
}

func (m *Manager) validateImmutableFields(initial, updated *models.Class) error {
	immutableFields := []immutableText{
		{
			name:     "class name",
			accessor: func(c *models.Class) string { return c.Class },
		},
		{
			name:     "vectorizer",
			accessor: func(c *models.Class) string { return c.Vectorizer },
		},
		{
			name:     "vector index type",
			accessor: func(c *models.Class) string { return c.VectorIndexType },
		},
	}

	for _, u := range immutableFields {
		if err := m.validateImmutableTextField(u, initial, updated); err != nil {
			return err
		}
	}

	if !reflect.DeepEqual(initial.Properties, updated.Properties) {
		return errors.Errorf(
			"properties cannot be updated through updating the class. Use the add " +
				"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
				"to add additional properties")
	}

	if !reflect.DeepEqual(initial.ModuleConfig, updated.ModuleConfig) {
		return errors.Errorf("module config is immutable")
	}

	return nil
}

type immutableText struct {
	accessor func(c *models.Class) string
	name     string
}

func (m *Manager) validateImmutableTextField(u immutableText,
	previous, next *models.Class,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from %q to %q",
			u.name, oldField, newField)
	}

	return nil
}

func (m *Manager) UpdateShardStatus(ctx context.Context, principal *models.Principal,
	className, shardName, targetStatus string,
) error {
	err := m.Authorizer.Authorize(principal, "update",
		fmt.Sprintf("schema/%s/shards/%s", className, shardName))
	if err != nil {
		return err
	}

	return m.migrator.UpdateShardStatus(ctx, className, shardName, targetStatus)
}
