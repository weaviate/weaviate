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
	"fmt"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/cluster/proto/api"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"golang.org/x/exp/slices"
)

type (
	NodeShardProcess map[string]*api.TenantsProcess
	metaClass        struct {
		sync.RWMutex
		Class          models.Class
		ClassVersion   uint64
		Sharding       sharding.State
		ShardVersion   uint64
		ShardProcesses map[string]NodeShardProcess
	}
)

func (m *metaClass) ClassInfo() (ci ClassInfo) {
	if m == nil {
		return
	}
	m.RLock()
	defer m.RUnlock()
	ci.Exists = true
	ci.Properties = len(m.Class.Properties)
	if m.Class.MultiTenancyConfig == nil {
		ci.MultiTenancy = models.MultiTenancyConfig{}
	} else {
		ci.MultiTenancy = *m.Class.MultiTenancyConfig
	}
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
		return "", 0, ErrShardNotFound
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
		return nil, 0, ErrShardNotFound
	}
	return slices.Clone(x.BelongsToNodes), m.version(), nil
}

// TenantsShards returns shard name for the provided tenant and its activity status
func (m *metaClass) TenantsShards(class string, tenants ...string) (map[string]string, uint64) {
	m.RLock()
	defer m.RUnlock()

	v := m.version()
	if !m.Sharding.PartitioningEnabled {
		return nil, v
	}

	res := make(map[string]string, len(tenants))
	for _, t := range tenants {
		if physical, ok := m.Sharding.Physical[t]; ok {
			res[t] = physical.ActivityStatus()
		}
	}
	return res, v
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
	mergedProps := MergeProps(m.Class.Properties, props)
	m.Class.Properties = mergedProps
	m.ClassVersion = v
	return nil
}

// MergeProps makes sure duplicates are not created by ignoring new props
// with the same names as old props.
// If property of nested type is present in both new and old slices,
// final property is created by merging new property into copy of old one
func MergeProps(old, new []*models.Property) []*models.Property {
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

func (m *metaClass) AddTenants(nodeID string, req *command.AddTenantsRequest, replFactor int64, v uint64) error {
	req.Tenants = removeNilTenants(req.Tenants)
	m.Lock()
	defer m.Unlock()

	// TODO-RAFT: Optimize here and avoid iteration twice on the req.Tenants array
	names := make([]string, len(req.Tenants))
	for i, tenant := range req.Tenants {
		names[i] = tenant.Name
	}
	// First determine the partition based on the node *present at the time of the log entry being created*
	partitions, err := m.Sharding.GetPartitions(req.ClusterNodes, names, replFactor)
	if err != nil {
		return fmt.Errorf("get partitions: %w", err)
	}

	// Iterate over requested tenants and assign them, if found, a partition
	for i, t := range req.Tenants {
		if _, ok := m.Sharding.Physical[t.Name]; ok {
			req.Tenants[i] = nil // already exists
			continue
		}
		// TODO-RAFT: Check in which cases can the partition not have assigned one to a tenant
		part, ok := partitions[t.Name]
		if !ok {
			// TODO-RAFT: Do we want to silently continue here or raise an error ?
			continue
		}
		p := sharding.Physical{Name: t.Name, Status: t.Status, BelongsToNodes: part}
		m.Sharding.Physical[t.Name] = p
		// TODO-RAFT: Check here why we set =nil if it is "owned by another node"
		if !slices.Contains(part, nodeID) {
			req.Tenants[i] = nil // is owned by another node
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

func (m *metaClass) UpdateTenantsProcess(nodeID string, req *command.TenantProcessRequest, v uint64) error {
	m.Lock()
	defer m.Unlock()

	name := req.Process.Tenant.Name
	if req.Action == command.TenantProcessRequest_ACTION_UNFREEZING {
		// on unfreezing get the requested status from the shard process
		if status := m.findRequestedStatus(nodeID, name, req.Action); status != "" {
			req.Process.Tenant.Status = status
		}
	}

	process := m.shardProcess(name, req.Action)
	process[req.Node] = req.Process

	shard, ok := m.Sharding.Physical[name]
	if !ok {
		return fmt.Errorf("shard %s not found", name)
	}

	if m.allShardProcessExecuted(name, req.Action) {
		m.applyShardProcess(name, req, &shard)
	} else {
		// ignore applying in case of aborts (upload action only)
		if !m.updateShardProcess(name, req, &shard) {
			req.Process = nil
			return nil
		}
	}

	m.ShardVersion = v
	m.Sharding.Physical[shard.Name] = shard
	if !slices.Contains(shard.BelongsToNodes, nodeID) {
		req.Process = nil
		return nil
	}
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

		// validate status
		switch p.ActivityStatus() {
		case req.Tenants[i].Status:
			req.Tenants[i] = nil
			continue
		case types.TenantActivityStatusFREEZING:
			if u.Status != models.TenantActivityStatusFROZEN {
				req.Tenants[i] = nil
				return n, fmt.Errorf("freezing is in progress, requested status: %s for tenant: %s", u.Status, u.Name)
			}
		case types.TenantActivityStatusUNFREEZING:
			if u.Status != models.TenantActivityStatusFROZEN {
				req.Tenants[i] = nil
				return n, fmt.Errorf("unfreezing is in progress, requested status: %s for tenant: %s", u.Status, u.Name)
			}
		}

		frozenShard := p.ActivityStatus() == models.TenantActivityStatusFROZEN
		toFrozen := u.Status == models.TenantActivityStatusFROZEN

		switch {
		case !toFrozen && frozenShard:
			if p, err = m.unfreeze(nodeID, i, req, p); err != nil {
				return n, err
			}
			if req.Tenants[i] != nil {
				u.Status = req.Tenants[i].Status
			}
		case toFrozen && !frozenShard:
			m.freeze(i, req, p)
		default:
			// do nothing
		}

		copy := p.DeepCopy()
		copy.Status = u.Status
		ps[copy.Name] = copy
		if !slices.Contains(copy.BelongsToNodes, nodeID) {
			req.Tenants[i] = nil
		}
		n++
	}

	if len(missingShards) > 0 {
		err = fmt.Errorf("%w: %v", ErrShardNotFound, missingShards)
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

func shardProcessID(name string, action command.TenantProcessRequest_Action) string {
	return fmt.Sprintf("%s-%s", name, action)
}

func (m *metaClass) findRequestedStatus(nodeID, name string, action command.TenantProcessRequest_Action) string {
	processes, pExists := m.ShardProcesses[shardProcessID(name, action)]
	if _, tExists := processes[nodeID]; pExists && tExists {
		return processes[nodeID].Tenant.Status
	}
	return ""
}

func (m *metaClass) allShardProcessExecuted(name string, action command.TenantProcessRequest_Action) bool {
	name = shardProcessID(name, action)
	expectedCount := len(m.ShardProcesses[name])
	i := 0
	for _, p := range m.ShardProcesses[name] {
		if p.Op > command.TenantsProcess_OP_START { // DONE or ABORT
			i++
		}
	}
	return i != 0 && i == expectedCount
}

func (m *metaClass) updateShardProcess(name string, req *command.TenantProcessRequest, copy *sharding.Physical) bool {
	processes := m.ShardProcesses[shardProcessID(name, req.Action)]
	switch req.Action {
	case command.TenantProcessRequest_ACTION_UNFREEZING:
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_DONE {
				copy.Status = sp.Tenant.Status
				break
			}
		}
		return false
	case command.TenantProcessRequest_ACTION_FREEZING:
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_ABORT {
				copy.Status = req.Process.Tenant.Status
				req.Process.Tenant.Status = sp.Tenant.Status
				return true
			}
		}
	default:
		return false
	}

	return false
}

func (m *metaClass) applyShardProcess(name string, req *command.TenantProcessRequest, copy *sharding.Physical) {
	processes := m.ShardProcesses[shardProcessID(name, req.Action)]

	switch req.Action {
	case command.TenantProcessRequest_ACTION_UNFREEZING:
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_DONE {
				copy.Status = sp.Tenant.Status
				req.Process.Tenant.Status = sp.Tenant.Status
				break
			}
		}
	case command.TenantProcessRequest_ACTION_FREEZING:
		count := 0
		onAbortStatus := copy.Status
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_DONE {
				count++
			} else {
				count--
				onAbortStatus = sp.Tenant.Status
			}
		}

		if count == len(processes) {
			copy.Status = req.Process.Tenant.Status
		} else {
			copy.Status = onAbortStatus
			req.Process.Tenant.Status = onAbortStatus
		}
	default:
		// do nothing
		return
	}
	delete(m.ShardProcesses, shardProcessID(name, req.Action))
}

func (m *metaClass) shardProcess(name string, action command.TenantProcessRequest_Action) map[string]*api.TenantsProcess {
	if len(m.ShardProcesses) == 0 {
		m.ShardProcesses = make(map[string]NodeShardProcess)
	}

	process, ok := m.ShardProcesses[shardProcessID(name, action)]
	if !ok {
		process = make(map[string]*api.TenantsProcess)
	}
	return process
}

func (m *metaClass) freeze(i int, req *command.UpdateTenantsRequest, shard sharding.Physical) {
	process := m.shardProcess(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_FREEZING)

	for _, node := range shard.BelongsToNodes {
		process[node] = &api.TenantsProcess{
			Op: command.TenantsProcess_OP_START,
		}
	}
	// to be proceed in the db layer
	req.Tenants[i].Status = types.TenantActivityStatusFREEZING
	m.ShardProcesses[shardProcessID(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_FREEZING)] = process
}

func (m *metaClass) unfreeze(nodeID string, i int, req *command.UpdateTenantsRequest, p sharding.Physical) (sharding.Physical, error) {
	name := req.Tenants[i].Name
	process := m.shardProcess(name, command.TenantProcessRequest_ACTION_UNFREEZING)

	partitions, err := m.Sharding.GetPartitions(req.ClusterNodes, []string{name}, m.Class.ReplicationConfig.Factor)
	if err != nil {
		req.Tenants[i] = nil
		return p, fmt.Errorf("get partitions: %w", err)
	}

	newNodes, ok := partitions[name]
	if !ok {
		req.Tenants[i] = nil
		return p, fmt.Errorf("can not assign new nodes to shard %s: %w", name, err)
	}

	oldNodes := p.BelongsToNodes
	p.Status = types.TenantActivityStatusUNFREEZING
	p.BelongsToNodes = newNodes

	newToOld := map[string]string{}
	slices.Sort(newNodes)
	slices.Sort(oldNodes)

	for idx, node := range newNodes {
		if idx >= len(oldNodes) {
			// ignore new nodes if the replication factor increase
			// and relay on replication client will replicate the data
			// after it's downloaded
			continue
		}
		newToOld[node] = oldNodes[idx]
		process[node] = &api.TenantsProcess{
			Op: command.TenantsProcess_OP_START,
			Tenant: &command.Tenant{
				Name:   name,
				Status: req.Tenants[i].Status, // requested status HOT, COLD
			},
		}
	}

	if _, exists := newToOld[nodeID]; !exists {
		// it does not belong to the new partitions
		req.Tenants[i] = nil
		return p, nil
	}
	m.ShardProcesses[shardProcessID(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_UNFREEZING)] = process
	req.Tenants[i].Name = fmt.Sprintf("%s#%s", req.Tenants[i].Name, newToOld[nodeID])
	req.Tenants[i].Status = p.Status
	return p, nil
}
