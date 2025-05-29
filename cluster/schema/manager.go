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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	gproto "google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	ErrBadRequest = errors.New("bad request")
	errDB         = errors.New("updating db")
	ErrSchema     = errors.New("updating schema")
)

type replicationFSM interface {
	HasOngoingReplication(collection string, shard string, replica string) bool
	DeleteReplicationsByCollection(collection string) error
	DeleteReplicationsByTenants(collection string, tenants []string) error
	SetUnCancellable(id uint64) error
}

type SchemaManager struct {
	schema         *schema
	db             Indexer
	parser         Parser
	log            *logrus.Logger
	replicationFSM replicationFSM
}

func NewSchemaManager(nodeId string, db Indexer, parser Parser, reg prometheus.Registerer, log *logrus.Logger) *SchemaManager {
	return &SchemaManager{
		schema: NewSchema(nodeId, db, reg),
		db:     db,
		parser: parser,
		log:    log,
	}
}

func (s *SchemaManager) NewSchemaReader() SchemaReader {
	return NewSchemaReader(
		s.schema,
		// Pass a versioned reader that will ignore all version and always return valid, we want to read the latest
		// state and not have to wait on a version
		VersionedSchemaReader{
			schema:        s.schema,
			WaitForUpdate: func(context.Context, uint64) error { return nil },
		},
	)
}

func (s *SchemaManager) NewSchemaReaderWithWaitFunc(f func(context.Context, uint64) error) SchemaReader {
	return NewSchemaReader(
		s.schema,
		VersionedSchemaReader{
			schema:        s.schema,
			WaitForUpdate: f,
		},
	)
}

func (s *SchemaManager) SetIndexer(idx Indexer) {
	s.db = idx
	s.schema.shardReader = idx
}

func (s *SchemaManager) SetReplicationFSM(fsm replicationFSM) {
	s.replicationFSM = fsm
}

func (s *SchemaManager) Snapshot() ([]byte, error) {
	var buf bytes.Buffer

	err := json.NewEncoder(&buf).Encode(s.schema.MetaClasses())
	return buf.Bytes(), err
}

func (s *SchemaManager) Restore(data []byte, parser Parser) error {
	return s.schema.Restore(data, parser)
}

func (s *SchemaManager) RestoreLegacy(data []byte, parser Parser) error {
	return s.schema.RestoreLegacy(data, parser)
}

func (s *SchemaManager) PreApplyFilter(req *command.ApplyRequest) error {
	classInfo := s.schema.ClassInfo(req.Class)

	// Discard restoring a class if it already exists
	if req.Type == command.ApplyRequest_TYPE_RESTORE_CLASS && classInfo.Exists {
		s.log.WithField("class", req.Class).Info("class already restored")
		return fmt.Errorf("class name %s already exists", req.Class)
	}

	// Discard adding class if the name already exists or a similar one exists
	if req.Type == command.ApplyRequest_TYPE_ADD_CLASS {
		if other := s.schema.ClassEqual(req.Class); other == req.Class {
			return fmt.Errorf("class name %s already exists", req.Class)
		} else if other != "" {
			return fmt.Errorf("%w: found similar class %q", ErrClassExists, other)
		}
	}

	return nil
}

func (s *SchemaManager) Load(ctx context.Context, nodeID string) error {
	if err := s.db.Open(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SchemaManager) ReloadDBFromSchema() {
	classes := s.schema.MetaClasses()

	cs := make([]command.UpdateClassRequest, len(classes))
	i := 0
	for _, v := range classes {
		migratePropertiesIfNecessary(&v.Class)
		cs[i] = command.UpdateClassRequest{Class: &v.Class, State: &v.Sharding}
		i++
	}
	s.db.TriggerSchemaUpdateCallbacks()
	s.log.Info("reload local db: update schema ...")
	s.db.ReloadLocalDB(context.Background(), cs)
}

func (s *SchemaManager) Close(ctx context.Context) (err error) {
	return s.db.Close(ctx)
}

func (s *SchemaManager) AddClass(cmd *command.ApplyRequest, nodeID string, schemaOnly bool, enableSchemaCallback bool) error {
	req := command.AddClassRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if req.State == nil {
		return fmt.Errorf("%w: nil sharding state", ErrBadRequest)
	}
	if err := s.parser.ParseClass(req.Class); err != nil {
		return fmt.Errorf("%w: parsing class: %w", ErrBadRequest, err)
	}
	req.State.SetLocalName(nodeID)
	// We need to make a copy of the sharding state to ensure that the state stored in the internal schema has no
	// references to. As we will make modification to it to reflect change in the sharding state (adding/removing
	// tenant) we don't want another goroutine holding a pointer to it and finding issues with concurrent read/writes.
	shardingStateCopy := req.State.DeepCopy()
	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.addClass(req.Class, &shardingStateCopy, cmd.Version) },
			updateStore:          func() error { return s.db.AddClass(req) },
			schemaOnly:           schemaOnly,
			enableSchemaCallback: enableSchemaCallback,
		},
	)
}

func (s *SchemaManager) RestoreClass(cmd *command.ApplyRequest, nodeID string, schemaOnly bool, enableSchemaCallback bool) error {
	req := command.AddClassRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if req.State == nil {
		return fmt.Errorf("%w: nil sharding state", ErrBadRequest)
	}
	if err := s.parser.ParseClass(req.Class); err != nil {
		return fmt.Errorf("%w: parsing class: %w", ErrBadRequest, err)
	}
	req.State.SetLocalName(nodeID)

	if err := s.db.RestoreClassDir(cmd.Class); err != nil {
		s.log.WithField("class", cmd.Class).WithError(err).
			Error("restore class directory from backup")
		// continue since we need to add class to the schema anyway
	}

	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.addClass(req.Class, req.State, cmd.Version) },
			updateStore:          func() error { return s.db.AddClass(req) },
			schemaOnly:           schemaOnly,
			enableSchemaCallback: enableSchemaCallback,
		},
	)
}

// ReplaceStatesNodeName it update the node name inside sharding states.
// WARNING: this shall be used in one node cluster environments only.
// because it will replace the shard node name if the node name got updated
// only if the replication factor is 1, otherwise it's no-op
func (s *SchemaManager) ReplaceStatesNodeName(new string) {
	s.schema.replaceStatesNodeName(new)
}

// UpdateClass modifies the vectors and inverted indexes associated with a class
// Other class properties are handled by separate functions
func (s *SchemaManager) UpdateClass(cmd *command.ApplyRequest, nodeID string, schemaOnly bool, enableSchemaCallback bool) error {
	req := command.UpdateClassRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if req.State != nil {
		req.State.SetLocalName(nodeID)
	}

	update := func(meta *metaClass) error {
		// Ensure that if non-default values for properties is stored in raft we fix them before processing an update to
		// avoid triggering diff on properties and therefore discarding a legitimate update.
		migratePropertiesIfNecessary(&meta.Class)
		u, err := s.parser.ParseClassUpdate(&meta.Class, req.Class)
		if err != nil {
			return fmt.Errorf("%w :parse class update: %w", ErrBadRequest, err)
		}
		meta.Class.VectorIndexConfig = u.VectorIndexConfig
		meta.Class.InvertedIndexConfig = u.InvertedIndexConfig
		meta.Class.VectorConfig = u.VectorConfig
		meta.Class.ReplicationConfig = u.ReplicationConfig
		meta.Class.MultiTenancyConfig = u.MultiTenancyConfig
		meta.Class.Description = u.Description
		meta.Class.Properties = u.Properties
		meta.ClassVersion = cmd.Version
		if req.State != nil {
			meta.Sharding = *req.State
		}
		return nil
	}

	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.updateClass(req.Class.Class, update) },
			updateStore:          func() error { return s.db.UpdateClass(req) },
			schemaOnly:           schemaOnly,
			enableSchemaCallback: enableSchemaCallback,
		},
	)
}

func (s *SchemaManager) DeleteClass(cmd *command.ApplyRequest, schemaOnly bool, enableSchemaCallback bool) error {
	var hasFrozen bool
	tenants, err := s.schema.getTenants(cmd.Class, nil)
	if err != nil {
		hasFrozen = false
	}

	for _, t := range tenants {
		if t.ActivityStatus == models.TenantActivityStatusFROZEN ||
			t.ActivityStatus == models.TenantActivityStatusFREEZING {
			hasFrozen = true
			break
		}
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { s.schema.deleteClass(cmd.Class); return nil },
			updateStore: func() error {
				if s.replicationFSM == nil {
					return fmt.Errorf("replication deleter is not set, this should never happen")
				} else if err := s.replicationFSM.DeleteReplicationsByCollection(cmd.Class); err != nil {
					// If there is an error deleting the replications then we log it but make sure not to block the deletion of the class from a UX PoV
					s.log.WithField("error", err).WithField("class", cmd.Class).Error("could not delete replication operations for deleted class")
				}
				return s.db.DeleteClass(cmd.Class, hasFrozen)
			},
			schemaOnly:           schemaOnly,
			enableSchemaCallback: enableSchemaCallback,
		},
	)
}

func (s *SchemaManager) AddProperty(cmd *command.ApplyRequest, schemaOnly bool, enableSchemaCallback bool) error {
	req := command.AddPropertyRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if len(req.Properties) == 0 {
		return fmt.Errorf("%w: empty property", ErrBadRequest)
	}

	return s.apply(
		applyOp{
			op:                   cmd.GetType().String(),
			updateSchema:         func() error { return s.schema.addProperty(cmd.Class, cmd.Version, req.Properties...) },
			updateStore:          func() error { return s.db.AddProperty(cmd.Class, req) },
			schemaOnly:           schemaOnly,
			enableSchemaCallback: enableSchemaCallback,
		},
	)
}

func (s *SchemaManager) UpdateShardStatus(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.UpdateShardStatusRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return nil },
			updateStore:  func() error { return s.db.UpdateShardStatus(&req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (s *SchemaManager) AddReplicaToShard(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.AddReplicaToShard{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return s.schema.addReplicaToShard(cmd.Class, cmd.Version, req.Shard, req.TargetNode) },
			updateStore: func() error {
				if req.TargetNode == s.schema.nodeID {
					return s.db.AddReplicaToShard(req.Class, req.Shard, req.TargetNode)
				}
				return nil
			},
			schemaOnly: schemaOnly,
		},
	)
}

func (s *SchemaManager) DeleteReplicaFromShard(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.DeleteReplicaFromShard{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op: cmd.GetType().String(),
			updateSchema: func() error {
				return s.schema.deleteReplicaFromShard(cmd.Class, cmd.Version, req.Shard, req.TargetNode)
			},
			updateStore: func() error {
				if req.TargetNode == s.schema.nodeID {
					return s.db.DeleteReplicaFromShard(req.Class, req.Shard, req.TargetNode)
				}
				return nil
			},
			schemaOnly: schemaOnly,
		},
	)
}

func (s *SchemaManager) AddTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.AddTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return s.schema.addTenants(cmd.Class, cmd.Version, req) },
			updateStore:  func() error { return s.db.AddTenants(cmd.Class, req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (s *SchemaManager) UpdateTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.UpdateTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op: cmd.GetType().String(),
			// updateSchema func will update the request's tenants and therefore we use it as a filter that is then sent
			// to the updateStore function. This allows us to effectively use the schema update to narrow down work for
			// the DB update.
			updateSchema: func() error { return s.schema.updateTenants(cmd.Class, cmd.Version, req, s.replicationFSM) },
			updateStore:  func() error { return s.db.UpdateTenants(cmd.Class, req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (s *SchemaManager) DeleteTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.DeleteTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	tenants, err := s.schema.getTenants(cmd.Class, req.Tenants)
	if err != nil {
		// error are handled by the updateSchema, so they are ignored here.
		// Instead, we log the error to detect tenant status before deleting
		// them from the schema. this allows the database layer to decide whether
		// to send the delete request to the cloud provider.
		s.log.WithFields(logrus.Fields{
			"class":   cmd.Class,
			"tenants": req.Tenants,
			"error":   err.Error(),
		}).Error("error getting tenants")
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return s.schema.deleteTenants(cmd.Class, cmd.Version, req) },
			updateStore: func() error {
				if s.replicationFSM == nil {
					return fmt.Errorf("replication deleter is not set, this should never happen")
				} else if err := s.replicationFSM.DeleteReplicationsByTenants(cmd.Class, req.Tenants); err != nil {
					// If there is an error deleting the replications then we log it but make sure not to block the deletion of the class from a UX PoV
					s.log.WithField("error", err).WithField("class", cmd.Class).WithField("tenants", tenants).Error("could not delete replication operations for deleted tenants")
				}
				return s.db.DeleteTenants(cmd.Class, tenants)
			},
			schemaOnly: schemaOnly,
		},
	)
}

func (s *SchemaManager) UpdateTenantsProcess(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.TenantProcessRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return s.schema.updateTenantsProcess(cmd.Class, cmd.Version, req) },
			updateStore:  func() error { return s.db.UpdateTenantsProcess(cmd.Class, req) },
			schemaOnly:   schemaOnly,
		},
	)
}

func (s *SchemaManager) SyncShard(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.SyncShardRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	if req.NodeId != s.schema.nodeID {
		return nil
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return nil },
			updateStore: func() error {
				return s.schema.Read(req.Collection, func(class *models.Class, state *sharding.State) error {
					physical, ok := state.Physical[req.Shard]
					// shard does not exist in the sharding state
					if !ok {
						// TODO: can we guarantee that the shard is not in use?
						// If so we should call s.db.DropShard(cmd.Class, req.Shard) here instead
						// For now, to be safe and avoid data loss, we just shut it down
						s.db.ShutdownShard(cmd.Class, req.Shard)
						// return early
						return nil
					}
					// if shard doesn't belong to this node
					if !slices.Contains(physical.BelongsToNodes, req.NodeId) {
						// shut it down
						s.db.ShutdownShard(cmd.Class, req.Shard)
						// return early
						return nil
					}
					// collection is single-tenant, shard is present, replica belongs to node
					if !state.PartitioningEnabled {
						// load it
						s.db.LoadShard(cmd.Class, req.Shard)
						// return early
						return nil
					}
					// collection is multi-tenant, shard is present, replica belongs to node
					switch physical.ActivityStatus() {
					// tenant is active
					case models.TenantActivityStatusACTIVE:
						// load it
						s.db.LoadShard(cmd.Class, req.Shard)
					// tenant is inactive
					case models.TenantActivityStatusINACTIVE:
						// shut it down
						s.db.ShutdownShard(cmd.Class, req.Shard)
					// tenant is in some other state
					default:
						// do nothing

					}
					return nil
				})
			},
			schemaOnly: schemaOnly,
		},
	)
}

func (s *SchemaManager) ReplicationAddReplicaToShard(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := command.ReplicationAddReplicaToShard{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return s.apply(
		applyOp{
			op: cmd.GetType().String(),
			updateSchema: func() error {
				err := s.replicationFSM.SetUnCancellable(req.OpId)
				if err != nil {
					return fmt.Errorf("set un-cancellable: %w", err)
				}
				return s.schema.addReplicaToShard(cmd.Class, cmd.Version, req.Shard, req.TargetNode)
			},
			updateStore: func() error {
				if req.TargetNode == s.schema.nodeID {
					return s.db.AddReplicaToShard(req.Class, req.Shard, req.TargetNode)
				}
				return nil
			},
			schemaOnly: schemaOnly,
		},
	)
}

type applyOp struct {
	op                   string
	updateSchema         func() error
	updateStore          func() error
	schemaOnly           bool
	enableSchemaCallback bool
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
func (s *SchemaManager) apply(op applyOp) error {
	if err := op.validate(); err != nil {
		return fmt.Errorf("could not validate raft apply op: %w", err)
	}

	// schema applied 1st to make sure any validation happen before applying it to db
	if err := op.updateSchema(); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrSchema, op.op, err)
	}

	if op.enableSchemaCallback && s.db != nil {
		// TriggerSchemaUpdateCallbacks is concurrent and at
		// this point of time schema shall be up to date.
		s.db.TriggerSchemaUpdateCallbacks()
	}

	if !op.schemaOnly {
		if err := op.updateStore(); err != nil {
			return fmt.Errorf("%w: %s: %w", errDB, op.op, err)
		}
	}

	return nil
}

// migratePropertiesIfNecessary migrate properties and set default values for them.
// This is useful when adding new properties to ensure that their default value is properly set.
// Current migrated properties:
// IndexRangeFilters was introduced with 1.26, so objects which were created
// on an older version, will have this value set to nil when the instance is
// upgraded. If we come across a property with nil IndexRangeFilters, it
// needs to be set as false, to avoid false positive class differences on
// comparison during class updates.
func migratePropertiesIfNecessary(class *models.Class) {
	for _, prop := range class.Properties {
		if prop.IndexRangeFilters == nil {
			prop.IndexRangeFilters = func() *bool { f := false; return &f }()
		}

		// Ensure we also migrate nested properties
		for _, nprop := range prop.NestedProperties {
			migrateNestedPropertiesIfNecessary(nprop)
		}
	}
}

func migrateNestedPropertiesIfNecessary(nprop *models.NestedProperty) {
	// migrate this nested property
	nprop.IndexRangeFilters = func() *bool { f := false; return &f }()
	// Recurse on all nested properties this one has
	for _, recurseNestedProperty := range nprop.NestedProperties {
		migrateNestedPropertiesIfNecessary(recurseNestedProperty)
	}
}
