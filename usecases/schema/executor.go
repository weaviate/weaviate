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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/schema"
)

type executor struct {
	store     metaReader
	migrator  Migrator
	callbacks []func(updatedSchema schema.Schema)
	logger    logrus.FieldLogger
}

// NewManager creates a new manager
func NewExecutor(migrator Migrator, mr metaReader,
	logger logrus.FieldLogger,
) (*executor, error) {
	m := &executor{
		migrator: migrator,
		logger:   logger,
		store:    mr,
	}

	return m, nil
}

func (m *executor) AddClass(pl cluster.AddClassRequest) error {
	// TODO-RAFT: do we need context here for every applyFunc?
	ctx := context.Background()
	if err := m.migrator.AddClass(ctx, pl.Class, pl.State); err != nil {
		return fmt.Errorf("apply add class: %w", err)
	}
	m.triggerSchemaUpdateCallbacks()
	return nil
}

func (m *executor) UpdateClass(req cluster.UpdateClassRequest) error {
	className := req.Class.Class
	ctx := context.Background()

	if hasTargetVectors(updated) {
		if err := m.migrator.UpdateVectorIndexConfigs(ctx, className, asVectorIndexConfigs(updated)); err != nil {
			return fmt.Errorf("vector index configs update: %w", err)
		}
	} else {
		if err := m.migrator.UpdateVectorIndexConfig(ctx,
			className, updated.VectorIndexConfig.(schema.VectorIndexConfig)); err != nil {
			return fmt.Errorf("vector index config update: %w", err)
		}
	}

	if err := m.migrator.UpdateInvertedIndexConfig(ctx, className,
		req.Class.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}
	m.triggerSchemaUpdateCallbacks()
	return nil
}

func (m *executor) DeleteClass(cls string) error {
	ctx := context.Background()
	if err := m.migrator.DropClass(ctx, cls); err != nil {
		m.logger.WithField("action", "delete_class").
			WithField("class", cls).Errorf("migrator: %v", err)
	}

	m.logger.WithField("action", "delete_class").WithField("class", cls).Debug("")
	m.triggerSchemaUpdateCallbacks()

	return nil
}

func (m *executor) AddProperty(className string, req cluster.AddPropertyRequest) error {
	ctx := context.Background()
	m.triggerSchemaUpdateCallbacks()
	return m.migrator.AddProperty(ctx, className, req.Property)
}

func (m *executor) AddTenants(class string, req *cluster.AddTenantsRequest) error {
	if len(req.Tenants) == 0 {
		return nil
	}
	updates := make([]*CreateTenantPayload, len(req.Tenants))
	for i, p := range req.Tenants {
		updates[i] = &CreateTenantPayload{
			Name:   p.Name,
			Status: p.Status,
		}
	}
	cls := m.store.ReadOnlyClass(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	ctx := context.Background()
	commit, err := m.migrator.NewTenants(ctx, cls, updates)
	if err != nil {
		return fmt.Errorf("migrator.new_tenants: %w", err)
	}
	commit(true) // commit new adding new tenant
	return nil
}

func (m *executor) UpdateTenants(class string, req *cluster.UpdateTenantsRequest) error {
	ctx := context.Background()
	cls := m.store.ReadOnlyClass(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}

	updates := make([]*UpdateTenantPayload, 0, len(req.Tenants))
	for _, tu := range req.Tenants {
		updates = append(updates, &UpdateTenantPayload{
			Name:   tu.Name,
			Status: tu.Status,
		})
	}

	commit, err := m.migrator.UpdateTenants(ctx, cls, updates)
	if err != nil {
		m.logger.WithField("action", "update_tenants").
			WithField("class", class).Error(err)
	}

	commit(true) // commit update of tenants
	return nil
}

func (m *executor) DeleteTenants(class string, req *cluster.DeleteTenantsRequest) error {
	ctx := context.Background()
	commit, err := m.migrator.DeleteTenants(ctx, class, req.Tenants)
	if err != nil {
		m.logger.WithField("action", "delete_tenants").
			WithField("class", class).Error(err)
	}

	commit(true) // commit deletion of tenants

	return nil
}

func (m *executor) triggerSchemaUpdateCallbacks() {
	s := m.store.ReadOnlySchema()
	for _, cb := range m.callbacks {
		cb(schema.Schema{
			Objects: &s,
		})
	}
}

// RegisterSchemaUpdateCallback allows other usecases to register a primitive
// type update callback. The callbacks will be called any time we persist a
// schema update
func (m *executor) RegisterSchemaUpdateCallback(callback func(updatedSchema schema.Schema)) {
	m.callbacks = append(m.callbacks, callback)
}
