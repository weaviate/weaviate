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

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	gproto "google.golang.org/protobuf/proto"
)

var (
	errBadRequest = errors.New("bad request")
	errDB         = errors.New("updating db")
	errSchema     = errors.New("updating schema")
)

type localDB struct {
	Schema *schema
	store  Indexer
	parser Parser
	log    *slog.Logger
}

func (db *localDB) SetIndexer(idx Indexer) {
	db.store = idx
	db.Schema.shardReader = idx
}

func (db *localDB) AddClass(cmd *command.ApplyRequest, nodeID string, schemaOnly bool) error {
	req := command.AddClassRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}
	if req.State == nil {
		return fmt.Errorf("%w: nil sharding state", errBadRequest)
	}
	if err := db.parser.ParseClass(req.Class); err != nil {
		return fmt.Errorf("%w: parsing class: %w", errBadRequest, err)
	}
	req.State.SetLocalName(nodeID)
	return db.apply(
		cmd.GetType().String(),
		func() error { return db.Schema.addClass(req.Class, req.State, cmd.Version) },
		func() error { return db.store.AddClass(req) },
		schemaOnly)
}

func (db *localDB) RestoreClass(cmd *command.ApplyRequest, nodeID string, schemaOnly bool) error {
	req := command.AddClassRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}
	if req.State == nil {
		return fmt.Errorf("%w: nil sharding state", errBadRequest)
	}
	if err := db.parser.ParseClass(req.Class); err != nil {
		return fmt.Errorf("%w: parsing class: %w", errBadRequest, err)
	}
	req.State.SetLocalName(nodeID)

	if err := db.store.RestoreClassDir(cmd.Class); err != nil {
		db.log.Error("restore class directory from backup %s: "+err.Error(), "class", cmd.Class)
		// continue since we need to add class to the schema anyway
	}

	return db.apply(
		cmd.GetType().String(),
		func() error { return db.Schema.addClass(req.Class, req.State, cmd.Version) },
		func() error { return db.store.AddClass(req) },
		schemaOnly)
}

// UpdateClass modifies the vectors and inverted indexes associated with a class
// Other class properties are handled by separate functions
func (db *localDB) UpdateClass(cmd *command.ApplyRequest, nodeID string, schemaOnly bool) error {
	req := command.UpdateClassRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}
	if req.State != nil {
		req.State.SetLocalName(nodeID)
	}

	update := func(meta *metaClass) error {
		u, err := db.parser.ParseClassUpdate(&meta.Class, req.Class)
		if err != nil {
			return fmt.Errorf("%w :parse class update: %w", errBadRequest, err)
		}
		meta.Class.VectorIndexConfig = u.VectorIndexConfig
		meta.Class.InvertedIndexConfig = u.InvertedIndexConfig
		meta.ClassVersion = cmd.Version
		return nil
	}

	return db.apply(
		cmd.GetType().String(),
		func() error { return db.Schema.updateClass(req.Class.Class, update) },
		func() error { return db.store.UpdateClass(req) },
		schemaOnly)
}

func (db *localDB) DeleteClass(cmd *command.ApplyRequest, schemaOnly bool) error {
	return db.apply(
		cmd.GetType().String(),
		func() error { db.Schema.deleteClass(cmd.Class); return nil },
		func() error { return db.store.DeleteClass(cmd.Class) },
		schemaOnly)
}

func (db *localDB) AddProperty(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.AddPropertyRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}
	if len(req.Properties) == 0 {
		return fmt.Errorf("%w: empty property", errBadRequest)
	}

	return db.apply(
		cmd.GetType().String(),
		func() error { return db.Schema.addProperty(cmd.Class, cmd.Version, req.Properties...) },
		func() error { return db.store.AddProperty(cmd.Class, req) },
		schemaOnly)
}

func (db *localDB) UpdateShardStatus(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.UpdateShardStatusRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return db.apply(
		cmd.GetType().String(),
		func() error { return nil },
		func() error { return db.store.UpdateShardStatus(&req) },
		schemaOnly)
}

func (db *localDB) AddTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.AddTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return db.apply(
		cmd.GetType().String(),
		func() error { return db.Schema.addTenants(cmd.Class, cmd.Version, req) },
		func() error { return db.store.AddTenants(cmd.Class, req) },
		schemaOnly)
}

func (db *localDB) UpdateTenants(cmd *command.ApplyRequest, schemaOnly bool) (n int, err error) {
	req := &command.UpdateTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return 0, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return n, db.apply(
		cmd.GetType().String(),
		func() error { n, err = db.Schema.updateTenants(cmd.Class, cmd.Version, req); return err },
		func() error { return db.store.UpdateTenants(cmd.Class, req) },
		schemaOnly)
}

func (db *localDB) DeleteTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.DeleteTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return db.apply(
		cmd.GetType().String(),
		func() error { return db.Schema.deleteTenants(cmd.Class, cmd.Version, req) },
		func() error { return db.store.DeleteTenants(cmd.Class, req) },
		schemaOnly)
}

func (db *localDB) Load(ctx context.Context, nodeID string) error {
	if err := db.store.Open(ctx); err != nil {
		return err
	}
	return nil
}

// Reload updates an already opened local database with the newest schema.
// It updates existing indexes and adds new ones as necessary
func (db *localDB) Reload() error {
	cs := make([]command.UpdateClassRequest, len(db.Schema.Classes))
	i := 0
	for _, v := range db.Schema.Classes {
		cs[i] = command.UpdateClassRequest{Class: &v.Class, State: &v.Sharding}
		i++
	}
	db.store.ReloadLocalDB(context.Background(), cs)
	return nil
}

func (db *localDB) Close(ctx context.Context) (err error) {
	return db.store.Close(ctx)
}

func (db *localDB) apply(op string, updateSchema, updateStore func() error, schemaOnly bool) error {
	if err := updateSchema(); err != nil {
		return fmt.Errorf("%w: %s: %w", errSchema, op, err)
	}

	if !schemaOnly && updateStore != nil {
		if err := updateStore(); err != nil {
			return fmt.Errorf("%w: %s: %w", errDB, op, err)
		}
	}
	return nil
}
