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
	"fmt"
	"strings"
	"sync"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"golang.org/x/exp/slices"
)

type metaClass struct {
	sync.RWMutex
	Class        models.Class
	ClassVersion uint64
	Sharding     sharding.State
	ShardVersion uint64
}

func (m *metaClass) ClassInfo() (ci ClassInfo) {
	if m == nil {
		return
	}
	m.RLock()
	defer m.RUnlock()
	ci.Exists = true
	ci.Properties = len(m.Class.Properties)
	ci.MultiTenancy, _ = m.MultiTenancyConfig()
	ci.ReplicationFactor = 1
	if m.Class.ReplicationConfig != nil && m.Class.ReplicationConfig.Factor > 1 {
		ci.ReplicationFactor = int(m.Class.ReplicationConfig.Factor)
	}
	ci.Tenants = len(m.Sharding.Physical)
	ci.ClassVersion = m.ClassVersion
	ci.ShardVersion = m.ShardVersion
	return ci
}

func (m *metaClass) version() uint64 {
	if m == nil {
		return 0
	}
	return max(m.ClassVersion, m.ShardVersion)
}

func (m *metaClass) MultiTenancyConfig() (mc models.MultiTenancyConfig, v uint64) {
	if m == nil {
		return
	}
	m.RLock()
	defer m.RUnlock()
	if m.Class.MultiTenancyConfig == nil {
		return
	}

	return *m.Class.MultiTenancyConfig, m.version()
}

// CloneClass returns a shallow copy of m
func (m *metaClass) CloneClass() *models.Class {
	m.RLock()
	defer m.RUnlock()
	cp := m.Class
	return &cp
}

// ShardOwner returns the node owner of the specified shard
func (m *metaClass) ShardOwner(shard string) (string, uint64, error) {
	m.RLock()
	defer m.RUnlock()
	x, ok := m.Sharding.Physical[shard]

	if !ok {
		return "", 0, errShardNotFound
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", 0, fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], m.version(), nil
}

// ShardFromUUID returns shard name of the provided uuid
func (m *metaClass) ShardFromUUID(uuid []byte) (string, uint64) {
	m.RLock()
	defer m.RUnlock()
	return m.Sharding.PhysicalShard(uuid), m.version()
}

// ShardReplicas returns the replica nodes of a shard
func (m *metaClass) ShardReplicas(shard string) ([]string, uint64, error) {
	m.RLock()
	defer m.RUnlock()
	x, ok := m.Sharding.Physical[shard]
	if !ok {
		return nil, 0, errShardNotFound
	}
	return slices.Clone(x.BelongsToNodes), m.version(), nil
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (m *metaClass) TenantsShards(class string, tenants ...string) map[string]string {
	m.RLock()
	defer m.RUnlock()

	if !m.Sharding.PartitioningEnabled {
		return nil
	}

	res := make(map[string]string, len(tenants))
	for _, t := range tenants {
		if physical, ok := m.Sharding.Physical[t]; ok {
			res[t] = physical.ActivityStatus()
		}
	}
	return res
}

// CopyShardingState returns a deep copy of the sharding state
func (m *metaClass) CopyShardingState() (*sharding.State, uint64) {
	m.RLock()
	defer m.RUnlock()
	st := m.Sharding.DeepCopy()
	return &st, m.version()
}

func (m *metaClass) AddProperty(v uint64, props ...*models.Property) error {
	m.Lock()
	defer m.Unlock()

	// update all at once to prevent race condition with concurrent readers
	mergedProps := mergeProps(m.Class.Properties, props)
	m.Class.Properties = mergedProps
	m.ClassVersion = v
	return nil
}

// mergeProps makes sure duplicates are not created by ignoring new props
// with the same names as old props.
// If property of nested type is present in both new and old slices,
// final property is created by merging new property into copy of old one
func mergeProps(old, new []*models.Property) []*models.Property {
	mergedProps := make([]*models.Property, len(old), len(old)+len(new))
	copy(mergedProps, old)

	// create memory to avoid duplication
	mem := make(map[string]int, len(old))
	for idx := range old {
		mem[strings.ToLower(old[idx].Name)] = idx
	}

	// pick ones not present in old slice or merge nested properties
	// if already present
	for idx := range new {
		if oldIdx, exists := mem[strings.ToLower(new[idx].Name)]; !exists {
			mergedProps = append(mergedProps, new[idx])
		} else {
			nestedProperties, merged := entSchema.MergeRecursivelyNestedProperties(
				mergedProps[oldIdx].NestedProperties,
				new[idx].NestedProperties)
			if merged {
				propCopy := *mergedProps[oldIdx]
				propCopy.NestedProperties = nestedProperties
				mergedProps[oldIdx] = &propCopy
			}
		}
	}

	return mergedProps
}

func (m *metaClass) AddTenants(nodeID string, req *command.AddTenantsRequest, v uint64) error {
	req.Tenants = removeNilTenants(req.Tenants)
	m.Lock()
	defer m.Unlock()

	for i, t := range req.Tenants {
		if _, ok := m.Sharding.Physical[t.Name]; ok {
			req.Tenants[i] = nil // already exists
			continue
		}

		p := sharding.Physical{Name: t.Name, Status: t.Status, BelongsToNodes: t.Nodes}
		m.Sharding.Physical[t.Name] = p
		if !slices.Contains(t.Nodes, nodeID) {
			req.Tenants[i] = nil // is owner by another node
		}
	}
	m.ShardVersion = v
	req.Tenants = removeNilTenants(req.Tenants)
	return nil
}

func (m *metaClass) DeleteTenants(req *command.DeleteTenantsRequest, v uint64) error {
	m.Lock()
	defer m.Unlock()

	for _, name := range req.Tenants {
		m.Sharding.DeletePartition(name)
	}
	m.ShardVersion = v
	return nil
}

func (m *metaClass) UpdateTenants(nodeID string, req *command.UpdateTenantsRequest, v uint64) (n int, err error) {
	m.Lock()
	defer m.Unlock()

	missingShards := []string{}
	ps := m.Sharding.Physical
	for i, u := range req.Tenants {

		p, ok := ps[u.Name]
		if !ok {
			missingShards = append(missingShards, u.Name)
			req.Tenants[i] = nil
			continue
		}
		if p.ActivityStatus() == u.Status {
			req.Tenants[i] = nil
			continue
		}
		copy := p.DeepCopy()
		copy.Status = u.Status
		if u.Nodes != nil && len(u.Nodes) >= 0 {
			copy.BelongsToNodes = u.Nodes
		}
		ps[u.Name] = copy
		if !slices.Contains(copy.BelongsToNodes, nodeID) {
			req.Tenants[i] = nil
		}
		n++
	}
	if len(missingShards) > 0 {
		err = fmt.Errorf("%w: %v", errShardNotFound, missingShards)
	}
	m.ShardVersion = v
	req.Tenants = removeNilTenants(req.Tenants)
	return
}

// LockGuard provides convenient mechanism for owning mutex by function which mutates the state.
func (m *metaClass) LockGuard(mutator func(*metaClass) error) error {
	m.Lock()
	defer m.Unlock()
	return mutator(m)
}

// RLockGuard provides convenient mechanism for owning mutex function which doesn't mutates the state
func (m *metaClass) RLockGuard(reader func(*models.Class, *sharding.State) error) error {
	m.RLock()
	defer m.RUnlock()
	return reader(&m.Class, &m.Sharding)
}
