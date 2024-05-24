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

	"github.com/sirupsen/logrus"
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
	log    *logrus.Logger
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
		applyOp{
			op:                    cmd.GetType().String(),
			updateSchema:          func() error { return db.Schema.addClass(req.Class, req.State, cmd.Version) },
			updateStore:           func() error { return db.store.AddClass(req) },
			schemaOnly:            schemaOnly,
			triggerSchemaCallback: true,
		},
	)
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
		db.log.WithField("class", cmd.Class).WithError(err).
			Error("restore class directory from backup")
		// continue since we need to add class to the schema anyway
	}

	return db.apply(
		applyOp{
			op:                    cmd.GetType().String(),
			updateSchema:          func() error { return db.Schema.addClass(req.Class, req.State, cmd.Version) },
			updateStore:           func() error { return db.store.AddClass(req) },
			schemaOnly:            schemaOnly,
			triggerSchemaCallback: true,
		},
	)
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
		meta.Class.VectorConfig = u.VectorConfig
		meta.Class.ReplicationConfig = u.ReplicationConfig
		meta.Class.MultiTenancyConfig = u.MultiTenancyConfig
		meta.ClassVersion = cmd.Version
		if req.State != nil {
			meta.Sharding = *req.State
		}
		return nil
	}

	return db.apply(
		applyOp{
			op:                    cmd.GetType().String(),
			updateSchema:          func() error { return db.Schema.updateClass(req.Class.Class, update) },
			updateStore:           func() error { return db.store.UpdateClass(req) },
			schemaOnly:            schemaOnly,
			triggerSchemaCallback: true,
		},
	)
}

func (db *localDB) DeleteClass(cmd *command.ApplyRequest, schemaOnly bool) error {
	return db.apply(
		applyOp{
			op:                    cmd.GetType().String(),
			updateSchema:          func() error { db.Schema.deleteClass(cmd.Class); return nil },
			updateStore:           func() error { return db.store.DeleteClass(cmd.Class) },
			schemaOnly:            schemaOnly,
			triggerSchemaCallback: true,
		},
	)
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
		applyOp{
			op:                    cmd.GetType().String(),
			updateSchema:          func() error { return db.Schema.addProperty(cmd.Class, cmd.Version, req.Properties...) },
			updateStore:           func() error { return db.store.AddProperty(cmd.Class, req) },
			schemaOnly:            schemaOnly,
			triggerSchemaCallback: true,
		},
	)
}

func (db *localDB) UpdateShardStatus(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.UpdateShardStatusRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return db.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return nil },
			updateStore:  func() error { return db.store.UpdateShardStatus(&req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (db *localDB) AddTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.AddTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return db.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return db.Schema.addTenants(cmd.Class, cmd.Version, req) },
			updateStore:  func() error { return db.store.AddTenants(cmd.Class, req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (db *localDB) UpdateTenants(cmd *command.ApplyRequest, schemaOnly bool) (n int, err error) {
	req := &command.UpdateTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return 0, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return n, db.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { n, err = db.Schema.updateTenants(cmd.Class, cmd.Version, req); return err },
			updateStore:  func() error { return db.store.UpdateTenants(cmd.Class, req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (db *localDB) DeleteTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.DeleteTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", errBadRequest, err)
	}

	return db.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return db.Schema.deleteTenants(cmd.Class, cmd.Version, req) },
			updateStore:  func() error { return db.store.DeleteTenants(cmd.Class, req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (db *localDB) Load(ctx context.Context, nodeID string) error {
	if err := db.store.Open(ctx); err != nil {
		return err
	}
	return nil
}

func (db *localDB) Close(ctx context.Context) (err error) {
	return db.store.Close(ctx)
}

type applyOp struct {
	op                    string
	updateSchema          func() error
	updateStore           func() error
	schemaOnly            bool
	triggerSchemaCallback bool
}

func (op applyOp) validate() error {
	if op.op == "" {
		return fmt.Errorf("op is not specified")
	}
	if op.updateSchema == nil {
		return fmt.Errorf("updateSchema func is nil")
	}
	if op.updateStore == nil {
		return fmt.Errorf("updateStore func is nil")
	}
	return nil
}

// apply does apply commands from RAFT to schema 1st and then db
func (db *localDB) apply(op applyOp) error {
	if err := op.validate(); err != nil {
		return fmt.Errorf("could not validate raft apply op: %s", err)
	}

	// schema applied 1st to make sure any validation happen before applying it to db
	if err := op.updateSchema(); err != nil {
		return fmt.Errorf("%w: %s: %w", errSchema, op.op, err)
	}

	if !op.schemaOnly {
		if err := op.updateStore(); err != nil {
			return fmt.Errorf("%w: %s: %w", errDB, op.op, err)
		}
	}

	// Always trigger the schema callback last
	if op.triggerSchemaCallback {
		db.store.TriggerSchemaUpdateCallbacks()
	}
	return nil
}
