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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

func (m *Manager) handleCommit(ctx context.Context, tx *cluster.Transaction) error {
	switch tx.Type {
	case AddClass, RepairClass:
		return m.handleAddClassCommit(ctx, tx)
	case AddProperty, RepairProperty:
		return m.handleAddPropertyCommit(ctx, tx)
	case mergeObjectProperty:
		return m.handleMergeObjectPropertyCommit(ctx, tx)
	case DeleteClass:
		return m.handleDeleteClassCommit(ctx, tx)
	case UpdateClass:
		return m.handleUpdateClassCommit(ctx, tx)
	case addTenants, RepairTenant:
		return m.handleAddTenantsCommit(ctx, tx)
	case updateTenants:
		return m.handleUpdateTenantsCommit(ctx, tx)
	case deleteTenants:
		return m.handleDeleteTenantsCommit(ctx, tx)
	case ReadSchema:
		return nil
	default:
		return errors.Errorf("unrecognized commit type %q", tx.Type)
	}
}

func (m *Manager) handleTxResponse(ctx context.Context,
	tx *cluster.Transaction,
) (data []byte, err error) {
	if tx.Type != ReadSchema {
		return nil, nil
	}
	m.schemaCache.RLockGuard(func() error {
		tx.Payload = ReadSchemaPayload{
			Schema: &m.schemaCache.State,
		}

		data, err = json.Marshal(tx)
		tx.Payload = ReadSchemaPayload{}
		return err
	})
	return
}

func (m *Manager) handleAddClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	pl, ok := tx.Payload.(AddClassPayload)
	if !ok {
		m.Unlock()
		return errors.Errorf("expected commit payload to be AddClassPayload, but got %T",
			tx.Payload)
	}

	err := m.handleAddClassCommitAndParse(ctx, &pl)
	m.Unlock()
	if err != nil {
		return err
	}
	// call to migrator needs to be outside the lock that is set in addClass
	return m.migrator.AddClass(ctx, pl.Class, pl.State)
}

func (m *Manager) handleAddClassCommitAndParse(ctx context.Context, pl *AddClassPayload) error {
	if pl.Class == nil {
		return fmt.Errorf("invalid tx: class is nil")
	}

	if pl.State == nil {
		return fmt.Errorf("invalid tx: state is nil")
	}

	err := m.parseShardingConfig(ctx, pl.Class)
	if err != nil {
		return err
	}

	err = m.parseVectorIndexConfig(ctx, pl.Class)
	if err != nil {
		return err
	}

	pl.State.SetLocalName(m.clusterState.LocalName())
	return m.addClassApplyChanges(ctx, pl.Class, pl.State)
}

func (m *Manager) handleAddPropertyCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(AddPropertyPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be AddPropertyPayload, but got %T",
			tx.Payload)
	}

	if pl.Property == nil {
		return fmt.Errorf("invalid tx: property is nil")
	}

	return m.addClassPropertyApplyChanges(ctx, pl.ClassName, pl.Property)
}

func (m *Manager) handleMergeObjectPropertyCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(MergeObjectPropertyPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be MergeObjectPropertyPayload, but got %T",
			tx.Payload)
	}

	if pl.Property == nil {
		return fmt.Errorf("invalid tx: property is nil")
	}

	return m.mergeClassObjectPropertyApplyChanges(ctx, pl.ClassName, pl.Property)
}

func (m *Manager) handleDeleteClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(DeleteClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be DeleteClassPayload, but got %T",
			tx.Payload)
	}

	return m.deleteClassApplyChanges(ctx, pl.ClassName)
}

func (m *Manager) handleUpdateClassCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	pl, ok := tx.Payload.(UpdateClassPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be UpdateClassPayload, but got %T",
			tx.Payload)
	}

	if pl.Class == nil {
		return fmt.Errorf("invalid tx: class is nil")
	}

	// note that a nil state may be valid on an update_class tx, whereas it's not
	// valid on a add_class. That's why we're not validating whether state is set
	// here

	if err := m.parseVectorIndexConfig(ctx, pl.Class); err != nil {
		return err
	}

	if err := m.parseShardingConfig(ctx, pl.Class); err != nil {
		return err
	}

	return m.updateClassApplyChanges(ctx, pl.ClassName, pl.Class, pl.State)
}

func (m *Manager) handleAddTenantsCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	req, ok := tx.Payload.(AddTenantsPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be AddTenants, but got %T",
			tx.Payload)
	}
	cls := m.getClassByName(req.Class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", req.Class, ErrNotFound)
	}

	err := m.onAddTenants(ctx, cls, req)
	if err != nil {
		m.logger.WithField("action", "on_add_tenants").
			WithField("n", len(req.Tenants)).
			WithField("class", cls.Class).Error(err)
	}
	return err
}

func (m *Manager) handleUpdateTenantsCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	req, ok := tx.Payload.(UpdateTenantsPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be UpdateTenants, but got %T",
			tx.Payload)
	}
	cls := m.getClassByName(req.Class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", req.Class, ErrNotFound)
	}

	err := m.onUpdateTenants(ctx, cls, req)
	if err != nil {
		m.logger.WithField("action", "on_add_tenants").
			WithField("n", len(req.Tenants)).
			WithField("class", cls.Class).Error(err)
	}
	return err
}

func (m *Manager) handleDeleteTenantsCommit(ctx context.Context,
	tx *cluster.Transaction,
) error {
	m.Lock()
	defer m.Unlock()

	req, ok := tx.Payload.(DeleteTenantsPayload)
	if !ok {
		return errors.Errorf("expected commit payload to be DeleteTenants, but got %T",
			tx.Payload)
	}
	cls := m.getClassByName(req.Class)
	if cls == nil {
		m.logger.WithField("action", "delete_tenants").
			WithField("class", req.Class).Warn("class not found")
		return nil
	}

	return m.onDeleteTenants(ctx, cls, req)
}
