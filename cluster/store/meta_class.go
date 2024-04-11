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
	ci.MultiTenancy = m.MultiTenancyConfig()
	ci.ReplicationFactor = 1
	if m.Class.ReplicationConfig != nil && m.Class.ReplicationConfig.Factor > 1 {
		ci.ReplicationFactor = int(m.Class.ReplicationConfig.Factor)
	}
	ci.Tenants = len(m.Sharding.Physical)
	ci.ClassVersion = m.ClassVersion
	ci.ShardVersion = m.ShardVersion
	return ci
}

func (m *metaClass) MultiTenancyConfig() (cfg models.MultiTenancyConfig) {
	if m == nil {
		return
	}
	m.RLock()
	defer m.RUnlock()
	if m.Class.MultiTenancyConfig == nil {
		return
	}

	cfg = *m.Class.MultiTenancyConfig
	return
}

// CloneClass returns a shallow copy of m
func (m *metaClass) CloneClass() *models.Class {
	m.RLock()
	defer m.RUnlock()
	cp := m.Class
	return &cp
}

// ShardOwner returns the node owner of the specified shard
func (m *metaClass) ShardOwner(shard string) (string, error) {
	m.RLock()
	defer m.RUnlock()
	x, ok := m.Sharding.Physical[shard]

	if !ok {
		return "", errShardNotFound
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], nil
}

// ShardFromUUID returns shard name of the provided uuid
func (m *metaClass) ShardFromUUID(uuid []byte) string {
	m.RLock()
	defer m.RUnlock()
	return m.Sharding.PhysicalShard(uuid)
}

// ShardReplicas returns the replica nodes of a shard
func (m *metaClass) ShardReplicas(shard string) ([]string, error) {
	m.RLock()
	defer m.RUnlock()
	x, ok := m.Sharding.Physical[shard]
	if !ok {
		return nil, errShardNotFound
	}
	return slices.Clone(x.BelongsToNodes), nil
}

// TenantShard returns shard name for the provided tenant and its activity status
func (m *metaClass) TenantShard(tenant string) (string, string) {
	m.RLock()
	defer m.RUnlock()

	if !m.Sharding.PartitioningEnabled {
		return "", ""
	}
	if physical, ok := m.Sharding.Physical[tenant]; ok {
		return tenant, physical.ActivityStatus()
	}
	return "", ""
}

// CopyShardingState returns a deep copy of the sharding state
func (m *metaClass) CopyShardingState() *sharding.State {
	m.RLock()
	defer m.RUnlock()
	st := m.Sharding.DeepCopy()
	return &st
}

func (m *metaClass) AddProperty(v uint64, props ...*models.Property) error {
	m.Lock()
	defer m.Unlock()

	// update all at once to prevent race condition with concurrent readers
	src := filterOutDuplicates(m.Class.Properties, props)
	dest := make([]*models.Property, len(src)+len(props))
	copy(dest, append(src, props...))
	m.Class.Properties = dest
	m.ClassVersion = v
	return nil
}

// filterOutDuplicates removes from the old any existing property could cause duplication
func filterOutDuplicates(old, new []*models.Property) []*models.Property {
	// create memory to avoid duplication
	var newUnique []*models.Property
	mem := make(map[string]int, len(new))
	for idx := range new {
		mem[strings.ToLower(new[idx].Name)] = idx
	}

	// pick only what is not in the new proprieties
	for idx := range old {
		if _, exists := mem[strings.ToLower(old[idx].Name)]; exists {
			continue
		}
		newUnique = append(newUnique, old[idx])
	}

	return newUnique
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
