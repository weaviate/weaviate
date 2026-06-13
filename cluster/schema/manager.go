//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	ErrBadRequest = errors.New("bad request")
	errDB         = errors.New("updating db")
	ErrSchema     = errors.New("updating schema")
)

type replicationFSM interface {
	HasActiveReplicationForShard(collection, shard string) bool
	HasActiveReplicationForCollection(collection string) bool
	DeleteReplicationsByCollection(collection string) error
	DeleteReplicationsByTenants(collection string, tenants []string) error
	SetUnCancellable(id uint64) error
}

// MutationGuard is consulted at RAFT-apply time for cross-FSM conflicts
// before a destructive schema mutation merges. Prevents bucket↔schema
// inversion when a schema mutation lands mid-reindex (DELETE searchable,
// UPDATE tokenization, DELETE class, DELETE/UPDATE tenants). See
// weaviate/0-weaviate-issues#218.
//
// FSM-determinism: implementations MUST be pure functions of their
// arguments + RAFT-replicated FSM state.
type MutationGuard interface {
	// CheckPropertyUpdate gates UpdateProperty. The migration completion
	// path bypasses this by setting FromInFlightMigration=true on its
	// own scheduled flip, since the task is still FINALIZING when the
	// flip lands.
	CheckPropertyUpdate(className, propertyName string) error

	// CheckClassMutation gates class-wide destructive ops (DeleteClass).
	// Stricter than CheckPropertyUpdate: any in-flight reindex on the
	// class blocks the mutation.
	CheckClassMutation(className string) error

	// CheckTenantMutation gates tenant transitions that make shards
	// locally unavailable. Transitions toward available are not a
	// conflict — callers must filter via
	// [tenantsTransitioningAwayFromActive] before invoking.
	CheckTenantMutation(className string, tenants []string) error
}

// Narrow slice of *cluster/distributedtask.Manager so schema doesn't
// depend on the full Manager surface and tests can stub it. nil-safe.
type distributedTaskCascadeDeleter interface {
	DeleteTasksForCollection(collection string) []distributedtask.TaskDescriptor
}

type SchemaManager struct {
	schema                 *schema
	db                     Indexer
	parser                 Parser
	log                    *logrus.Logger
	replicationFSM         replicationFSM
	mutationGuard          MutationGuard
	distributedTaskManager distributedTaskCascadeDeleter
}

func NewSchemaManager(nodeId string, db Indexer, parser Parser, reg prometheus.Registerer, log *logrus.Logger) *SchemaManager {
	return &SchemaManager{
		schema: NewSchema(nodeId, db, reg),
		db:     db,
		parser: parser,
		log:    log,
	}
}

// SetMutationGuard installs the [MutationGuard] consulted by
// [SchemaManager.UpdateProperty] before merging a property update.
// Pass nil to remove the guard. Safe to call once at startup after
// both this manager and the guard's backing FSM exist (today: the
// distributed-task FSM Manager, wired in cluster/store.go's NewFSM).
//
// Subsequent calls overwrite the previous registration. The setter
// itself is not synchronized — it must run during single-threaded
// FSM bootstrap, before any apply path can read the field.
func (s *SchemaManager) SetMutationGuard(g MutationGuard) {
	s.mutationGuard = g
}

// tenantsTransitioningAwayFromActive returns the names of tenants in
// the UpdateTenants payload that would transition AWAY from
// locally-available state (INACTIVE / OFFLOADED / FROZEN /
// transitional aliases). Returns an empty slice if every tenant is
// transitioning to a locally-available state (ACTIVE / HOT /
// ONLOADING / UNFREEZING) — in that case no MutationGuard check
// fires, matching the "transitions toward available are safe"
// semantics.
//
// Used by [SchemaManager.UpdateTenants] to narrow the guard
// invocation to tenants whose data would actually become locally
// unavailable mid-reindex (the only case where the bucket↔schema
// inversion family of bugs is reachable).
func tenantsTransitioningAwayFromActive(tenants []*command.Tenant) []string {
	if len(tenants) == 0 {
		return nil
	}
	var affected []string
	for _, t := range tenants {
		switch t.GetStatus() {
		case models.TenantActivityStatusINACTIVE,
			models.TenantActivityStatusOFFLOADED,
			models.TenantActivityStatusOFFLOADING,
			models.TenantActivityStatusFROZEN,
			models.TenantActivityStatusFREEZING,
			models.TenantActivityStatusCOLD:
			affected = append(affected, t.GetName())
		}
	}
	return affected
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

// Wires the cascade-delete used by [SchemaManager.DeleteClass]. nil-safe;
// the cascade is a no-op when unset (bootstrap / partial-harness tests).
func (s *SchemaManager) SetDistributedTaskManager(m distributedTaskCascadeDeleter) {
	s.distributedTaskManager = m
}

func (s *SchemaManager) SchemaSnapshot() ([]byte, error) {
	var buf bytes.Buffer

	err := json.NewEncoder(&buf).Encode(s.schema.MetaClasses())
	return buf.Bytes(), err
}

func (s *SchemaManager) AliasSnapshot() ([]byte, error) {
	var buf bytes.Buffer

	err := json.NewEncoder(&buf).Encode(s.schema.aliases)
	return buf.Bytes(), err
}

func (s *SchemaManager) Restore(data []byte, parser Parser) error {
	return s.schema.Restore(data, parser)
}

func (s *SchemaManager) RestoreAliases(data []byte) error {
	return s.schema.RestoreAlias(data)
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
		other, isAlias := s.schema.ClassEqual(req.Class)
		item := "class"
		if isAlias {
			item = "alias"
		}

		if other == req.Class {
			return fmt.Errorf("%s name %s already exists", item, req.Class)
		} else if other != "" {
			return fmt.Errorf("%w: found similar %s %q", ErrClassExists, item, other)
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
	// dupa
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if req.State == nil {
		return fmt.Errorf("%w: nil sharding state", ErrBadRequest)
	}
	// Validate xref existence. DataType is pre-qualified on NS clusters.
	for _, prop := range req.Class.Properties {
		if !entSchema.IsRefDataType(prop.DataType) {
			// don't need to validate non-xref data types
			continue
		}
		for _, dt := range prop.DataType {
			if dt == req.Class.Class {
				// self-references are always allowed
				continue
			}
			if s.schema.classExists(dt) {
				// class exists, all good
				continue
			}
			// class does not exist, error out
			return fmt.Errorf("%w: %w", ErrBadRequest, entSchema.ErrRefToNonexistentClass)
		}

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

		// A structural vector change can't be reconciled by an in-flight movement's snapshot+CCL.
		if s.replicationFSM != nil && s.replicationFSM.HasActiveReplicationForCollection(meta.Class.Class) {
			if reason := dangerousVectorConfigChange(&meta.Class, u); reason != "" {
				return fmt.Errorf("%w: %w: %s on collection %q; retry after it completes",
					ErrBadRequest, ErrReplicaMovementInProgress, reason, meta.Class.Class)
			}
		}

		// Capture previous and updated replication factors
		var initialRF int64
		if meta.Class.ReplicationConfig != nil {
			initialRF = meta.Class.ReplicationConfig.Factor
		}

		var updatedRF int64
		if u.ReplicationConfig != nil {
			updatedRF = u.ReplicationConfig.Factor
		}

		// validate replication factor increase
		if initialRF < updatedRF {
			for _, physical := range meta.Sharding.Physical {
				if int64(len(physical.BelongsToNodes)) < updatedRF {
					return fmt.Errorf(
						"not enough replicas in shard %q to increase replication factor to %d for class %q",
						physical.Name,
						updatedRF,
						meta.Class.Class,
					)
				}
			}
		}

		// Apply updates
		meta.Class.VectorIndexConfig = u.VectorIndexConfig
		meta.Class.InvertedIndexConfig = u.InvertedIndexConfig
		meta.Class.VectorConfig = u.VectorConfig
		meta.Class.ReplicationConfig = u.ReplicationConfig
		meta.Class.MultiTenancyConfig = u.MultiTenancyConfig
		meta.Class.ObjectTTLConfig = u.ObjectTTLConfig
		meta.Class.Description = u.Description
		meta.Class.Properties = u.Properties
		meta.ClassVersion = cmd.Version

		if req.State != nil {
			meta.Sharding = *req.State
		}

		// update sharding replication factor
		if u.ReplicationConfig != nil {
			meta.Sharding.ReplicationFactor = updatedRF
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
	// Cross-FSM mutation guard — see [MutationGuard.CheckClassMutation].
	if s.mutationGuard != nil {
		if err := s.mutationGuard.CheckClassMutation(cmd.Class); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
	}

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
			op: cmd.GetType().String(),
			updateSchema: func() error {
				s.schema.deleteClass(cmd.Class)
				// Cascade lives in updateSchema (not updateStore) so it
				// also runs on schemaOnly catchup replay: DTM is in-memory
				// FSM state, rebuilt from RAFT log on restart, and the
				// DELETE_CLASS apply MUST drop tasks the replay just
				// re-added. weaviate/0-weaviate-issues#231.
				s.cascadeDeleteDistributedTasks(cmd.Class)
				return nil
			},
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

func (s *SchemaManager) cascadeDeleteDistributedTasks(class string) {
	if s.distributedTaskManager == nil {
		s.log.WithField("class", class).
			Debug("distributed-task manager not set; skipping cascade-delete on class delete")
		return
	}
	removed := s.distributedTaskManager.DeleteTasksForCollection(class)
	if len(removed) == 0 {
		return
	}
	s.log.WithField("class", class).
		WithField("removed_count", len(removed)).
		Info("cascade-deleted distributed-task records for dropped class")
	// IsLevelEnabled gates the allocation, not just the emit.
	if s.log.IsLevelEnabled(logrus.DebugLevel) {
		ids := make([]string, 0, len(removed))
		for _, d := range removed {
			ids = append(ids, fmt.Sprintf("%s/%d", d.ID, d.Version))
		}
		s.log.WithField("class", class).
			WithField("removed_tasks", ids).
			Debug("cascade-deleted distributed-task IDs (full list)")
	}
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

func (s *SchemaManager) UpdateProperty(cmd *command.ApplyRequest, schemaOnly bool, enableSchemaCallback bool) error {
	req := command.UpdatePropertyRequest{}
	if err := json.Unmarshal(cmd.SubCommand, &req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if req.Property == nil {
		return fmt.Errorf("%w: empty property", ErrBadRequest)
	}

	// Cross-FSM mutation guard — see [MutationGuard.CheckPropertyUpdate].
	// FromInFlightMigration=true bypasses the guard for the migration's
	// own scheduled schema flip; see the bypass clause in that godoc.
	if s.mutationGuard != nil && !req.FromInFlightMigration {
		if err := s.mutationGuard.CheckPropertyUpdate(cmd.Class, req.Property.Name); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
	}

	// Disabling an index deletes the property's LSM buckets, which an in-flight movement's
	// snapshot+CCL can't reconcile. FromInFlightMigration bypasses, as with the reindex guard.
	if s.replicationFSM != nil && !req.FromInFlightMigration &&
		s.replicationFSM.HasActiveReplicationForCollection(cmd.Class) {
		if cls, _ := s.schema.ReadOnlyClass(cmd.Class); cls != nil {
			if old := findProp(cls, req.Property.Name); old != nil && disablesAnyIndex(old, req.Property) {
				return fmt.Errorf("%w: %w: property %q index removal blocked on collection %q; retry after it completes",
					ErrBadRequest, ErrReplicaMovementInProgress, req.Property.Name, cmd.Class)
			}
		}
	}

	return s.apply(
		applyOp{
			op: cmd.GetType().String(),
			updateSchema: func() error {
				_, err := s.schema.updateProperty(cmd.Class, cmd.Version, req.Property, req.FieldsToUpdate)
				return err
			},
			updateStore:          func() error { return s.db.UpdateProperty(cmd.Class, req) },
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

	// Cross-FSM mutation guard — see [MutationGuard.CheckTenantMutation].
	// Per the asymmetric-semantics clause in that godoc, narrow to
	// tenants whose transition would make shards locally unavailable
	// before invoking; transitions toward available are skipped.
	if s.mutationGuard != nil {
		affectedTenants := tenantsTransitioningAwayFromActive(req.GetTenants())
		if len(affectedTenants) > 0 {
			if err := s.mutationGuard.CheckTenantMutation(cmd.Class, affectedTenants); err != nil {
				return fmt.Errorf("%w: %w", ErrBadRequest, err)
			}
		}
	}

	return s.apply(
		applyOp{
			op: cmd.GetType().String(),
			// updateSchema func will update the request's tenants and therefore we use it as a filter that is then sent
			// to the updateStore function. This allows us to effectively use the schema update to narrow down work for
			// the DB update.
			updateSchema:          func() error { return s.schema.updateTenants(cmd.Class, cmd.Version, req, s.replicationFSM) },
			updateStore:           func() error { return s.db.UpdateTenants(cmd.Class, req) },
			schemaOnly:            schemaOnly,
			allowPartialSchemaErr: true,
		},
	)
}

func (s *SchemaManager) DeleteTenants(cmd *command.ApplyRequest, schemaOnly bool) error {
	req := &command.DeleteTenantsRequest{}
	if err := gproto.Unmarshal(cmd.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Cross-FSM mutation guard — see [MutationGuard.CheckTenantMutation].
	// Deleting tenants destroys their shard state unconditionally, so
	// no toward-available filtering applies here (cf. UpdateTenants).
	if s.mutationGuard != nil && len(req.Tenants) > 0 {
		if err := s.mutationGuard.CheckTenantMutation(cmd.Class, req.Tenants); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
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

	var physical *sharding.Physical
	var partitioningEnabled bool
	err := s.NewSchemaReader().Read(req.Collection, true, func(class *models.Class, state *sharding.State) error {
		partitioningEnabled = state.PartitioningEnabled
		p, ok := state.Physical[req.Shard]
		if !ok {
			// no physical, leave var as nil
			return nil
		}
		physical = &p
		return nil
	})
	if err != nil {
		return fmt.Errorf("read schema for sync shard: %w", err)
	}

	return s.apply(
		applyOp{
			op:           cmd.GetType().String(),
			updateSchema: func() error { return nil },
			updateStore: func() error {
				// shard does not exist in the sharding state
				if physical == nil {
					// TODO: can we guarantee that the shard is not in use?
					// If so we should call s.db.DropShard(cmd.Class, req.Shard) here instead
					// For now, to be safe and avoid data loss, we just shut it down
					s.db.ShutdownShard(cmd.Class, req.Shard)
					return nil
				}
				// shard is present but replica doesn't belong to this node
				if !slices.Contains(physical.BelongsToNodes, req.NodeId) {
					s.db.ShutdownShard(cmd.Class, req.Shard)
					return nil
				}
				// collection is ST, shard is present, and replica belongs to node
				if !partitioningEnabled {
					s.db.LoadShard(cmd.Class, req.Shard)
					return nil
				}
				// collection is MT, shard is present, and replica belongs to node
				switch physical.ActivityStatus() {
				case models.TenantActivityStatusACTIVE:
					s.db.LoadShard(cmd.Class, req.Shard)
				case models.TenantActivityStatusINACTIVE:
					s.db.ShutdownShard(cmd.Class, req.Shard)
				default:
					// do nothing
				}
				return nil
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
	// allowPartialSchemaErr, when true, allows updateStore to proceed when
	// updateSchema returns a *PartialUpdateError. This is used for operations
	// where the schema layer filters out invalid entries (e.g. missing or
	// transitional-state tenants) and the DB must still be updated for the
	// entries that were successfully applied.
	// The schema error is returned to the caller after updateStore completes.
	allowPartialSchemaErr bool
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
	schemaErr := op.updateSchema()
	if schemaErr != nil {
		var partialErr *PartialUpdateError
		if !op.allowPartialSchemaErr || !errors.As(schemaErr, &partialErr) {
			return fmt.Errorf("%w: %s: %w", ErrSchema, op.op, schemaErr)
		}
	}

	if op.enableSchemaCallback && s.db != nil {
		// TriggerSchemaUpdateCallbacks is concurrent and at
		// this point of time schema shall be up to date.
		s.db.TriggerSchemaUpdateCallbacks()
	}

	if !op.schemaOnly {
		if err := op.updateStore(); err != nil {
			if schemaErr != nil {
				// Both the schema update (partial) and the DB update failed.
				// Return both so the caller is informed of what was skipped
				// and what failed.
				return fmt.Errorf("%w: %s: %w; %w: %s: %w",
					ErrSchema, op.op, schemaErr,
					errDB, op.op, err)
			}
			return fmt.Errorf("%w: %s: %w", errDB, op.op, err)
		}
	}

	if schemaErr != nil {
		return fmt.Errorf("%w: %s: %w", ErrSchema, op.op, schemaErr)
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
