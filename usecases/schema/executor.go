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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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

func (e *executor) Open(ctx context.Context) error {
	return e.migrator.WaitForStartup(ctx)
}

func (e *executor) Close(ctx context.Context) error {
	return e.migrator.Shutdown(ctx)
}

func (e *executor) AddClass(pl cluster.AddClassRequest) error {
	// TODO-RAFT: do we need context here for every applyFunc?
	ctx := context.Background()
	if err := e.migrator.AddClass(ctx, pl.Class, pl.State); err != nil {
		return fmt.Errorf("apply add class: %w", err)
	}
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) UpdateClass(req cluster.UpdateClassRequest) error {
	className := req.Class.Class
	ctx := context.Background()
	if err := e.migrator.UpdateVectorIndexConfig(ctx,
		className, req.Class.VectorIndexConfig.(schemaConfig.VectorIndexConfig)); err != nil {
		return errors.Wrap(err, "vector index config")
	}

	if err := e.migrator.UpdateInvertedIndexConfig(ctx, className,
		req.Class.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) DeleteClass(cls string) error {
	ctx := context.Background()
	if err := e.migrator.DropClass(ctx, cls); err != nil {
		e.logger.WithField("action", "delete_class").
			WithField("class", cls).Errorf("migrator: %v", err)
	}

	e.logger.WithField("action", "delete_class").WithField("class", cls).Debug("")
	e.triggerSchemaUpdateCallbacks()

	return nil
}

func (e *executor) AddProperty(className string, req cluster.AddPropertyRequest) error {
	ctx := context.Background()
	e.triggerSchemaUpdateCallbacks()
	return e.migrator.AddProperty(ctx, className, req.Property)
}

func (e *executor) AddTenants(class string, req *cluster.AddTenantsRequest) error {
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
	cls := e.store.ReadOnlyClass(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	ctx := context.Background()
	commit, err := e.migrator.NewTenants(ctx, cls, updates)
	if err != nil {
		return fmt.Errorf("migrator.new_tenants: %w", err)
	}
	commit(true) // commit new adding new tenant
	return nil
}

func (e *executor) UpdateTenants(class string, req *cluster.UpdateTenantsRequest) error {
	ctx := context.Background()
	cls := e.store.ReadOnlyClass(class)
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

	commit, err := e.migrator.UpdateTenants(ctx, cls, updates)
	if err != nil {
		e.logger.WithField("action", "update_tenants").
			WithField("class", class).Error(err)
	}

	commit(true) // commit update of tenants
	return nil
}

func (e *executor) DeleteTenants(class string, req *cluster.DeleteTenantsRequest) error {
	ctx := context.Background()
	commit, err := e.migrator.DeleteTenants(ctx, class, req.Tenants)
	if err != nil {
		e.logger.WithField("action", "delete_tenants").
			WithField("class", class).Error(err)
	}

	commit(true) // commit deletion of tenants

	return nil
}

func (e *executor) UpdateShardStatus(req *cluster.UpdateShardStatusRequest) error {
	ctx := context.Background()
	return e.migrator.UpdateShardStatus(ctx, req.Class, req.Shard, req.Status)
}

// TODO-RAFT START
// change GetShardsStatus() to accept a tenant parameter
// TODO-RAFT END

func (e *executor) GetShardsStatus(class string) (models.ShardStatusList, error) {
	ctx := context.Background()
	shardsStatus, err := e.migrator.GetShardsStatus(ctx, class, "") // tenant needed here
	if err != nil {
		return nil, err
	}

	resp := models.ShardStatusList{}

	for name, status := range shardsStatus {
		resp = append(resp, &models.ShardStatusGetResponse{
			Name:   name,
			Status: status,
		})
	}

	return resp, nil
}

func (e *executor) triggerSchemaUpdateCallbacks() {
	s := e.store.ReadOnlySchema()
	for _, cb := range e.callbacks {
		cb(schema.Schema{
			Objects: &s,
		})
	}
}

// RegisterSchemaUpdateCallback allows other usecases to register a primitive
// type update callback. The callbacks will be called any time we persist a
// schema update
func (e *executor) RegisterSchemaUpdateCallback(callback func(updatedSchema schema.Schema)) {
	e.callbacks = append(e.callbacks, callback)
}
