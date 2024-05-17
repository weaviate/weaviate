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
		// TODO: fix PushShard issues before enabling scale out
		//       https://github.com/weaviate/weaviate/issues/4840
		// meta.Class.ReplicationConfig = u.ReplicationConfig
		meta.Class.MultiTenancyConfig = u.MultiTenancyConfig
		meta.ClassVersion = cmd.Version
		// TODO: fix PushShard issues before enabling scale out
		//       https://github.com/weaviate/weaviate/issues/4840
		// if req.State != nil {
		// 	meta.Sharding = *req.State
		// }
		return nil
	}

	return db.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return db.Schema.updateClass(req.Class.Class, update) },
			updateStore:  func() error { return db.store.UpdateClass(req) },
			schemaOnly:   schemaOnly,
			// Apply the DB change last otherwise we will error on the parsing of the class while updating the store.
			// We need the schema to first parse the update and apply it so that we can use it in the DB update.
			applyDbUpdateFirst:    false,
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
			applyDbUpdateFirst:    true,
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
			op:           cmd.GetType().String(),
			updateSchema: func() error { return db.Schema.addProperty(cmd.Class, cmd.Version, req.Properties...) },
			updateStore:  func() error { return db.store.AddProperty(cmd.Class, req) },
			schemaOnly:   schemaOnly,
			// Apply the DB first to ensure the underlying buckets related to properties are created/deleted *before* the
			// schema is updated. This allows us to have object write waiting on the right schema version to proceed only
			// once the buck buckets are present.
			applyDbUpdateFirst:    true,
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
			op:                 cmd.GetType().String(),
			updateSchema:       func() error { return nil },
			updateStore:        func() error { return db.store.UpdateShardStatus(&req) },
			schemaOnly:         schemaOnly,
			applyDbUpdateFirst: true,
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

type applyOp struct {
	op                    string
	updateSchema          func() error
	updateStore           func() error
	schemaOnly            bool
	applyDbUpdateFirst    bool
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

func (db *localDB) apply(op applyOp) error {
	if err := op.validate(); err != nil {
		return fmt.Errorf("could not validate raft apply op: %s", err)
	}

	// To avoid a if/else with repeated logic, setup op1 and op2 to either updateSchema or updateStore depending on
	// op.applyDbUpdateFirst and op.schemaOnly
	op1, op2 := op.updateSchema, op.updateStore
	msg1, msg2 := errSchema, errDB
	if op.applyDbUpdateFirst && !op.schemaOnly {
		op1, op2 = op.updateStore, op.updateSchema
		msg1, msg2 = errDB, errSchema
	}

	if err := op1(); err != nil {
		return fmt.Errorf("%w: %s: %w", msg1, op.op, err)
	}

	// If the operation is schema only, op1 is always the schemaUpdate so we can skip op2
	if !op.schemaOnly {
		if err := op2(); err != nil {
			return fmt.Errorf("%w: %s: %w", msg2, op.op, err)
		}
	}

	// Always trigger the schema callback last
	if op.triggerSchemaCallback {
		db.store.TriggerSchemaUpdateCallbacks()
	}
	return nil
}
