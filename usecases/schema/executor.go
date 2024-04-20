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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

type executor struct {
	store           metaReader
	migrator        Migrator
	callbacks       []func(updatedSchema schema.Schema)
	logger          logrus.FieldLogger
	restoreClassDir func(string) error
}

// NewManager creates a new manager
func NewExecutor(migrator Migrator, mr metaReader,
	logger logrus.FieldLogger, classBackupDir func(string) error,
) *executor {
	return &executor{
		migrator:        migrator,
		logger:          logger,
		store:           mr,
		restoreClassDir: classBackupDir,
	}
}

func (e *executor) Open(ctx context.Context) error {
	return e.migrator.WaitForStartup(ctx)
}

// ReloadLocalDB reloads the local database using the latest schema.
func (e *executor) ReloadLocalDB(ctx context.Context, all []api.UpdateClassRequest) error {
	cs := make([]*models.Class, len(all))

	for i, u := range all {
		e.logger.WithField("index", u.Class.Class).Info("restore local index")
		cs[i] = u.Class
		if err := e.migrator.UpdateIndex(ctx, u.Class, u.State); err != nil {
			return fmt.Errorf("restore index %q: %w", i, err)
		}
	}
	e.rebuildGQL(models.Schema{Classes: cs})
	return nil
}

func (e *executor) Close(ctx context.Context) error {
	return e.migrator.Shutdown(ctx)
}

func (e *executor) AddClass(pl api.AddClassRequest) error {
	ctx := context.Background()
	if err := e.migrator.AddClass(ctx, pl.Class, pl.State); err != nil {
		return fmt.Errorf("apply add class: %w", err)
	}
	e.triggerSchemaUpdateCallbacks()
	return nil
}

// RestoreClassDir restores classes on the filesystem directly from the temporary class backup stored on disk.
// This function is invoked by the Raft store when a restoration request is sent by the backup coordinator.
func (e *executor) RestoreClassDir(class string) error {
	return e.restoreClassDir(class)
}

func (e *executor) UpdateClass(req api.UpdateClassRequest) error {
	className := req.Class.Class
	ctx := context.Background()

	if hasTargetVectors(req.Class) {
		if err := e.migrator.UpdateVectorIndexConfigs(ctx, className, asVectorIndexConfigs(req.Class)); err != nil {
			return fmt.Errorf("vector index configs update: %w", err)
		}
	} else {
		if err := e.migrator.UpdateVectorIndexConfig(ctx,
			className, req.Class.VectorIndexConfig.(schemaConfig.VectorIndexConfig)); err != nil {
			return fmt.Errorf("vector index config update: %w", err)
		}
	}

	if err := e.migrator.UpdateInvertedIndexConfig(ctx, className,
		req.Class.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) UpdateIndex(req api.UpdateClassRequest) error {
	ctx := context.Background()
	if err := e.migrator.UpdateIndex(ctx, req.Class, req.State); err != nil {
		return err
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

func (e *executor) AddProperty(className string, req api.AddPropertyRequest) error {
	ctx := context.Background()
	if err := e.migrator.AddProperty(ctx, className, req.Properties...); err != nil {
		return err
	}

	e.logger.WithField("action", "add_property").WithField("class", className)
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) AddTenants(class string, req *api.AddTenantsRequest) error {
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
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) UpdateTenants(class string, req *api.UpdateTenantsRequest) error {
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
		return err
	}

	commit(true) // commit update of tenants
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) DeleteTenants(class string, req *api.DeleteTenantsRequest) error {
	ctx := context.Background()
	commit, err := e.migrator.DeleteTenants(ctx, class, req.Tenants)
	if err != nil {
		e.logger.WithField("action", "delete_tenants").
			WithField("class", class).Error(err)
	}

	commit(true) // commit deletion of tenants
	e.triggerSchemaUpdateCallbacks()
	return nil
}

func (e *executor) UpdateShardStatus(req *api.UpdateShardStatusRequest) error {
	if err := e.migrator.UpdateShardStatus(context.Background(), req.Class, req.Shard, req.Status, req.SchemaVersion); err != nil {
		return err
	}
	e.triggerSchemaUpdateCallbacks()
	return nil
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

func (e *executor) rebuildGQL(s models.Schema) {
	for _, cb := range e.callbacks {
		cb(schema.Schema{
			Objects: &s,
		})
	}
}

func (e *executor) triggerSchemaUpdateCallbacks() {
	e.rebuildGQL(e.store.ReadOnlySchema())
}

// RegisterSchemaUpdateCallback allows other usecases to register a primitive
// type update callback. The callbacks will be called any time we persist a
// schema update
func (e *executor) RegisterSchemaUpdateCallback(callback func(updatedSchema schema.Schema)) {
	e.callbacks = append(e.callbacks, callback)
}
