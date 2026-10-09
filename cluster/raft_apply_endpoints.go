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

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
	"google.golang.org/protobuf/proto"
)

func (s *Raft) AddClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}

	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateClass(ctx context.Context, cls *models.Class, _ *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}

	req := cmd.UpdateClassRequest{Class: cls}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}

	return s.Execute(ctx, command)
}

func (s *Raft) DeleteClass(ctx context.Context, name string) (uint64, error) {
	command := &cmd.ApplyRequest{
		Type:  cmd.ApplyRequest_TYPE_DELETE_CLASS,
		Class: name,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) RestoreClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name: %w", schema.ErrBadRequest)
	}
	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_RESTORE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) AddProperty(ctx context.Context, class string, props ...*models.Property) (uint64, error) {
	for _, p := range props {
		if p == nil || p.Name == "" || class == "" {
			return 0, fmt.Errorf("empty property or empty class name: %w", schema.ErrBadRequest)
		}
	}
	req := cmd.AddPropertyRequest{Properties: props}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// UpdateProperty schedules a RAFT command to merge `property` into the
// named class. When `fields` is non-empty, the FSM merges ONLY the listed
// fields onto an existing property (see api.PropertyField* constants); the
// rest are kept from the existing class state, so two concurrent updaters
// that touch different fields cannot clobber each other. An empty `fields`
// preserves the legacy "replace every field" semantics — public API
// callers that don't pass a mask are unaffected.
//
// This is the public entry point: callers reach it from the REST / gRPC
// schema handlers. The cluster-side MutationGuard at apply time blocks
// updates while a reindex on the same (collection, property) is STARTED
// or FINALIZING. Internal callers driven by the distributed-task
// scheduler's own completion path must use
// [Raft.UpdatePropertyFromMigration] instead, which sets the bypass
// flag.
func (s *Raft) UpdateProperty(ctx context.Context, class string, property *models.Property, fields ...string) (uint64, error) {
	return s.updateProperty(ctx, class, property, false, fields...)
}

// UpdatePropertyFromMigration schedules a RAFT command for a property
// schema flip emitted by the in-process distributed-task scheduler's
// own completion handler (see
// adapters/repos/db/reindex_provider.flipSemanticMigrationSchema). The
// resulting command carries [api.UpdatePropertyRequest.FromInFlightMigration]
// = true, which the schema FSM uses to bypass the in-flight-reindex
// MutationGuard for this single update.
//
// Public REST / gRPC handlers must not call this; they go through
// [Raft.UpdateProperty]. The migration-only bypass exists because the
// scheduler's OnTaskCompleted fires while the task is still FINALIZING
// (status not yet FINISHED), so the same MutationGuard that protects
// the property from external mutations would otherwise reject the
// migration's own scheduled flip.
func (s *Raft) UpdatePropertyFromMigration(ctx context.Context, class string, property *models.Property, fields ...string) (uint64, error) {
	return s.updateProperty(ctx, class, property, true, fields...)
}

func (s *Raft) updateProperty(ctx context.Context, class string, property *models.Property, fromInFlightMigration bool, fields ...string) (uint64, error) {
	if class == "" || property == nil {
		return 0, fmt.Errorf("empty property or empty class name: %w", schema.ErrBadRequest)
	}
	req := cmd.UpdatePropertyRequest{
		Property:              property,
		FieldsToUpdate:        fields,
		FromInFlightMigration: fromInFlightMigration,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) AddReplicaToShard(ctx context.Context, class, shard, targetNode string) (uint64, error) {
	if class == "" || shard == "" || targetNode == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.AddReplicaToShard{
		Class:      class,
		Shard:      shard,
		TargetNode: targetNode,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) DeleteReplicaFromShard(ctx context.Context, class, shard, targetNode string) (uint64, error) {
	if class == "" || shard == "" || targetNode == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.DeleteReplicaFromShard{
		Class:      class,
		Shard:      shard,
		TargetNode: targetNode,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) ReplicationAddReplicaToShard(ctx context.Context, class, shard, targetNode string, opId uint64) (uint64, error) {
	if class == "" || shard == "" || targetNode == "" {
		return 0, fmt.Errorf("empty class or shard or sourceNode or targetNode: %w", schema.ErrBadRequest)
	}
	req := cmd.ReplicationAddReplicaToShard{
		Class:      class,
		Shard:      shard,
		TargetNode: targetNode,
		OpId:       opId,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateShardStatus(ctx context.Context, class, shard, status string) (uint64, error) {
	if class == "" || shard == "" {
		return 0, fmt.Errorf("empty class or shard: %w", schema.ErrBadRequest)
	}
	req := cmd.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) AddTenants(ctx context.Context, class string, req *cmd.AddTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}

	filtered, dropped := withoutKnownTenants(s.SchemaReader(), class, req)
	if dropped > 0 {
		s.store.metrics.tenantAddsFiltered.Add(float64(dropped))
	}
	if len(filtered.Tenants) == 0 {
		// Nothing to commit, so there is no version for the caller to wait on.
		s.store.metrics.tenantAddProposalsSkipped.Inc()
		return 0, nil
	}
	req = filtered

	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

// knownTenantsReader is the local schema read withoutKnownTenants needs;
// [schema.SchemaReader] satisfies it.
type knownTenantsReader interface {
	KnownTenants(class string, tenants []string) map[string]struct{}
}

// withoutKnownTenants drops the tenants the local schema already lists, leaving
// req untouched when it cannot tell, and reports how many it dropped.
//
// metaClass.AddTenants nils out an existing tenant on apply, so such an entry
// buys a full consensus round trip only to be discarded. That round trip is not
// cheap: 244ms mean and ~2.5s p99 in production, and auto-tenant creation
// re-asserts every tenant on every object write, so one node issued 35k adds
// against 6.6k real tenants in 43 minutes. A tenant the local schema is missing
// is still sent, so a lagging node behaves exactly as it does today.
func withoutKnownTenants(reader knownTenantsReader, class string, req *cmd.AddTenantsRequest) (*cmd.AddTenantsRequest, int) {
	names := make([]string, 0, len(req.Tenants))
	for _, tenant := range req.Tenants {
		if tenant != nil {
			names = append(names, tenant.Name)
		}
	}

	known := reader.KnownTenants(class, names)
	if len(known) == 0 {
		return req, 0
	}

	dropped := 0
	fresh := make([]*cmd.Tenant, 0, len(req.Tenants))
	for _, tenant := range req.Tenants {
		if tenant == nil {
			continue
		}
		if _, ok := known[tenant.Name]; ok {
			dropped++
			continue
		}
		fresh = append(fresh, tenant)
	}

	// A new request rather than a copy: a proto message carries state that must
	// not be copied by value.
	return &cmd.AddTenantsRequest{ClusterNodes: req.ClusterNodes, Tenants: fresh}, dropped
}

func (s *Raft) UpdateTenants(ctx context.Context, class string, req *cmd.UpdateTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) DeleteTenants(ctx context.Context, class string, req *cmd.DeleteTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) UpdateTenantsProcess(ctx context.Context, class string, req *cmd.TenantProcessRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request: %w", schema.ErrBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_TENANT_PROCESS,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(ctx, command)
}

func (s *Raft) Execute(ctx context.Context, req *cmd.ApplyRequest) (uint64, error) {
	t := prometheus.NewTimer(
		monitoring.GetMetrics().SchemaWrites.WithLabelValues(
			req.Type.String(),
		))
	defer t.ObserveDuration()

	var schemaVersion uint64
	err := backoff.Retry(func() error {
		var err error

		// Validate the apply first
		if _, ok := cmd.ApplyRequest_Type_name[int32(req.Type.Number())]; !ok {
			err = types.ErrUnknownCommand
			// This is an invalid apply command, don't retry
			return backoff.Permanent(err)
		}

		// We are the leader, let's apply
		if s.store.IsLeader() {
			schemaVersion, err = s.store.Execute(req)
			// We might fail due to leader not found as we are losing or transferring leadership, retry
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				return err
			}
			// Transient by construction, so retry instead of failing the client.
			if errors.Is(err, types.ErrFSMNotCaughtUp) {
				return err
			}
			return backoff.Permanent(err)
		}

		leader := s.store.Leader()
		if leader == "" {
			err = s.leaderErr()
			s.log.Warnf("apply: could not find leader: %s", err)
			return err
		}

		var resp *cmd.ApplyResponse
		resp, err = s.cl.Apply(ctx, leader, req)
		if err != nil {
			// Don't retry if the actual apply to the leader failed, we have retry at the network layer already
			return backoff.Permanent(err)
		}
		schemaVersion = resp.Version
		return nil
		// pass in the election timeout after applying multiplier
	}, backoffConfig(ctx, s.store.raftConfig().ElectionTimeout))

	return schemaVersion, err
}
