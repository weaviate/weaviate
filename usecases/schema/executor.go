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
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type executor struct {
	schemaReader SchemaReader
	migrator     Migrator

	callbacksLock sync.RWMutex
	callbacks     []func(updatedSchema schema.Schema)

	logger          logrus.FieldLogger
	restoreClassDir func(string) error
}

// NewManager creates a new manager
func NewExecutor(migrator Migrator, sr SchemaReader,
	logger logrus.FieldLogger, classBackupDir func(string) error,
) *executor {
	return &executor{
		migrator:        migrator,
		logger:          logger,
		schemaReader:    sr,
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
	e.TriggerSchemaUpdateCallbacks()
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
			className, asVectorIndexConfig(req.Class)); err != nil {
			return fmt.Errorf("vector index config update: %w", err)
		}
	}

	if err := e.migrator.UpdateInvertedIndexConfig(ctx, className,
		req.Class.InvertedIndexConfig); err != nil {
		return errors.Wrap(err, "inverted index config")
	}

	if err := e.migrator.UpdateReplicationFactor(ctx, className, req.Class.ReplicationConfig.Factor); err != nil {
		return fmt.Errorf("replication index update: %w", err)
	}

	return nil
}

func (e *executor) UpdateIndex(req api.UpdateClassRequest) error {
	ctx := context.Background()
	if err := e.migrator.UpdateIndex(ctx, req.Class, req.State); err != nil {
		return err
	}
	return nil
}

func (e *executor) DeleteClass(cls string) error {
	ctx := context.Background()
	if err := e.migrator.DropClass(ctx, cls); err != nil {
		e.logger.WithFields(logrus.Fields{
			"action": "delete_class",
			"class":  cls,
		}).WithError(err).Errorf("migrator")
	}

	e.logger.WithFields(logrus.Fields{
		"action": "delete_class",
		"class":  cls,
	}).Debug("deleting class")

	return nil
}

func (e *executor) AddProperty(className string, req api.AddPropertyRequest) error {
	ctx := context.Background()
	if err := e.migrator.AddProperty(ctx, className, req.Properties...); err != nil {
		return err
	}

	e.logger.WithFields(logrus.Fields{
		"action": "add_property",
		"class":  className,
	}).Debug("adding property")
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
	cls := e.schemaReader.ReadOnlyClass(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}
	ctx := context.Background()
	if err := e.migrator.NewTenants(ctx, cls, updates); err != nil {
		return fmt.Errorf("migrator.new_tenants: %w", err)
	}
	return nil
}

func (e *executor) UpdateTenants(class string, req *api.UpdateTenantsRequest) error {
	ctx := context.Background()
	cls := e.schemaReader.ReadOnlyClass(class)
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

	if err := e.migrator.UpdateTenants(ctx, cls, updates); err != nil {
		e.logger.WithFields(logrus.Fields{
			"action": "update_tenants",
			"class":  class,
		}).WithError(err).Error("error updating tenants")
		return err
	}
	return nil
}

func (e *executor) DeleteTenants(class string, req *api.DeleteTenantsRequest) error {
	ctx := context.Background()
	if err := e.migrator.DeleteTenants(ctx, class, req.Tenants); err != nil {
		e.logger.WithFields(logrus.Fields{
			"action": "delete_tenants",
			"class":  class,
		}).WithError(err).Error("error deleting tenants")
	}

	return nil
}

func (e *executor) UpdateShardStatus(req *api.UpdateShardStatusRequest) error {
	ctx := context.Background()
	return e.migrator.UpdateShardStatus(ctx, req.Class, req.Shard, req.Status, req.SchemaVersion)
}

func (e *executor) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	ctx := context.Background()
	shardsStatus, err := e.migrator.GetShardsStatus(ctx, class, tenant)
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

func (e *executor) TriggerSchemaUpdateCallbacks() {
	e.callbacksLock.RLock()
	defer e.callbacksLock.RUnlock()

	s := e.schemaReader.ReadOnlySchema()
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
	e.callbacksLock.Lock()
	defer e.callbacksLock.Unlock()

	e.callbacks = append(e.callbacks, callback)
}
