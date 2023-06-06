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

	if err := sharding.ValidateConfigUpdate(initial.ShardingConfig.(sharding.Config),
		updated.ShardingConfig.(sharding.Config), m.clusterState); err != nil {
		return errors.Wrap(err, "sharding config")
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

func (m *Manager) updateClassApplyChanges(ctx context.Context, className string,
	updated *models.Class, updatedShardingState *sharding.State,
) error {
	if err := m.migrator.UpdateVectorIndexConfig(ctx,
		className, updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
		return errors.Wrap(err, "vector index config")
	}

	if err := m.migrator.UpdateInvertedIndexConfig(ctx, className,
		updated.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}

	initial := m.getClassByName(className)
	if initial == nil {
		return ErrNotFound
	}

	*initial = *updated

	if updatedShardingState != nil {
		// do not override if transaction does not contain an updated state

		// the sharding state caches the node name, we must therefore set this
		// explicitly now.
		updatedShardingState.SetLocalName(m.clusterState.LocalName())
		m.shardingStateLock.Lock()
		m.state.ShardingState[className] = updatedShardingState
		m.shardingStateLock.Unlock()
	}

	return m.saveSchema(ctx, nil)
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

// Below here is old - to be deleted

// UpdateObject which exists
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal,
	name string, class *models.Class,
) error {
	err := m.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.updateClass(ctx, name, class)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClass(ctx context.Context, className string,
	class *models.Class,
) error {
	m.Lock()
	defer m.Unlock()

	var newName *string

	if class.Class != className {
		// the name in the URI and body don't match, so we assume the user wants to rename
		n := schema.UppercaseClassName(class.Class)
		newName = &n
	}

	var err error
	class, err = schema.GetClassByName(m.state.ObjectSchema, className)
	if err != nil {
		return err
	}

	classNameAfterUpdate := className

	// First validate the request
	if newName != nil {
		err = m.validateClassNameUniqueness(*newName)
		classNameAfterUpdate = *newName
		if err != nil {
			return err
		}
	}

	// Validate name / keywords in contextionary
	if err = m.validateClassName(ctx, classNameAfterUpdate); err != nil {
		return err
	}

	// Validated! Now apply the changes.
	class.Class = classNameAfterUpdate

	err = m.saveSchema(ctx, nil)

	if err != nil {
		return nil
	}

	return m.migrator.UpdateClass(ctx, className, newName)
}
