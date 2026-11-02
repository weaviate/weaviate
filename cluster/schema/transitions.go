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
	"encoding/json"
	"fmt"
	"slices"

	gproto "google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type TenantTransition struct {
	Class        string
	Tenant       string
	CloudAttempt uint64
	Transition   *sharding.Transition
}

func (s *SchemaManager) RunningTransitions() []TenantTransition {
	return s.schema.runningTransitions()
}

func (s *schema) runningTransitions() []TenantTransition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var running []TenantTransition
	for class, meta := range s.classes {
		meta.RLock()
		for name, p := range meta.Sharding.Physical {
			if p.Transition != nil {
				running = append(running, TenantTransition{
					Class: class, Tenant: name, CloudAttempt: p.CloudAttempt,
					Transition: p.Transition.DeepCopy(),
				})
			}
		}
		meta.RUnlock()
	}
	return running
}

func hasCloudCopy(t *models.Tenant) bool {
	switch t.ActivityStatus {
	case models.TenantActivityStatusFROZEN, models.TenantActivityStatusFREEZING, models.TenantActivityStatusUNFREEZING:
		return true
	default:
		return false
	}
}

// SetNodeAlive installs the leader's liveness check. Call once during FSM
// bootstrap.
func (s *SchemaManager) SetNodeAlive(alive func(node string) bool) {
	s.nodeAlive = alive
}

// refuseDuringTransition runs on the leader before the append, because node
// liveness is not replicated.
func (s *SchemaManager) refuseDuringTransition(req *command.ApplyRequest) error {
	switch req.Type {
	case command.ApplyRequest_TYPE_DELETE_TENANT:
		sub := &command.DeleteTenantsRequest{}
		if err := gproto.Unmarshal(req.SubCommand, sub); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		return s.refuseTransitioning(req.Class, sub.Tenants)
	case command.ApplyRequest_TYPE_UPDATE_TENANT:
		sub := &command.UpdateTenantsRequest{}
		if err := gproto.Unmarshal(req.SubCommand, sub); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		return s.refuseTransitionStart(req.Class, sub)
	case command.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD:
		sub := command.AddReplicaToShard{}
		if err := json.Unmarshal(req.SubCommand, &sub); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		return s.refuseTransitioning(sub.Class, []string{sub.Shard})
	case command.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD:
		sub := command.DeleteReplicaFromShard{}
		if err := json.Unmarshal(req.SubCommand, &sub); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		return s.refuseTransitioning(sub.Class, []string{sub.Shard})
	case command.ApplyRequest_TYPE_REPLICATION_REPLICATE:
		sub := command.ReplicationReplicateShardRequest{}
		if err := json.Unmarshal(req.SubCommand, &sub); err != nil {
			return fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		return s.refuseTransitioning(sub.SourceCollection, []string{sub.SourceShard})
	default:
		return nil
	}
}

func (s *SchemaManager) refuseTransitioning(class string, tenants []string) error {
	return s.schema.readTenants(class, tenants, func(name string, p sharding.Physical) error {
		if p.Transition != nil {
			return fmt.Errorf("%w: tenant %q has a running %s; retry when it ends", ErrTenantTransitionalState, name, p.Transition.Kind)
		}
		return nil
	})
}

// refuseTransitionStart refuses a freeze or unfreeze that would wait on a down
// node. It chooses an unfreeze's nodes the way the apply does.
func (s *SchemaManager) refuseTransitionStart(class string, req *command.UpdateTenantsRequest) error {
	meta := s.schema.metaClass(class)
	if meta == nil || s.nodeAlive == nil {
		return nil
	}
	return meta.RLockGuard(func(cls *models.Class, ss *sharding.State) error {
		for _, t := range req.Tenants {
			p, ok := ss.Physical[t.Name]
			if !ok || p.Transition != nil {
				continue
			}
			frozen := p.Status == models.TenantActivityStatusFROZEN
			var participants []string
			switch {
			case t.Status == models.TenantActivityStatusFROZEN && !frozen:
				participants = p.BelongsToNodes
			case t.Status != models.TenantActivityStatusFROZEN && frozen:
				partitions, err := ss.GetPartitions(req.ClusterNodes, []string{t.Name}, cls.ReplicationConfig.Factor)
				if err != nil {
					continue // the apply reports it
				}
				participants = partitions[t.Name]
			}
			for _, node := range participants {
				if !s.nodeAlive(node) {
					return fmt.Errorf("%w: tenant %q would wait on down node %q", ErrTenantTransitionalState, t.Name, node)
				}
			}
		}
		return nil
	})
}

func (s *schema) readTenants(class string, tenants []string, read func(string, sharding.Physical) error) error {
	s.mu.RLock()
	meta := s.classes[class]
	s.mu.RUnlock()
	if meta == nil {
		return nil
	}

	return meta.RLockGuard(func(_ *models.Class, ss *sharding.State) error {
		for name := range slices.Values(tenants) {
			if p, ok := ss.Physical[name]; ok {
				if err := read(name, p); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
