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
	NodesCloudStatus map[string]*api.TenantsProcess
	metaClass        struct {
		sync.RWMutex
		Class          models.Class
		ClassVersion   uint64
		Sharding       sharding.State
		ShardVersion   uint64
		ShardProcesses map[string]NodesCloudStatus
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

	if len(m.ShardProcesses) == 0 {
		m.ShardProcesses = make(map[string]NodesCloudStatus)
	}

	tn, ok := m.ShardProcesses[req.Process.Tenant.Name]
	if !ok {
		tn = make(NodesCloudStatus)
	}

	if req.Process.Tenant.Status == types.TenantActivityStatusUNFROZEN {
		_, texsists := m.ShardProcesses[req.Process.Tenant.Name]
		if _, exists := m.ShardProcesses[req.Process.Tenant.Name][nodeID]; texsists && exists {
			req.Process.Tenant.Status = m.ShardProcesses[req.Process.Tenant.Name][nodeID].Tenant.Status
		}
	}

	tn[req.Node] = req.Process

	shallBe := len(m.ShardProcesses[req.Process.Tenant.Name])
	x := 0
	for _, pro := range m.ShardProcesses[req.Process.Tenant.Name] {
		if pro.Op > command.TenantsProcess_OP_START { // DONE or ABORT
			x++
		}
	}

	copy := m.Sharding.Physical[req.Process.Tenant.Name].DeepCopy() // mooga todo handle not found

	if x != 0 && x == shallBe {
		// completed uploading/downloading
		copy.Status = req.Process.Tenant.Status

		for _, sProcess := range m.ShardProcesses[req.Process.Tenant.Name] {
			if sProcess.Op == command.TenantsProcess_OP_ABORT && req.Process.Tenant.Status != types.TenantActivityStatusUNFROZEN {
				// force tenant status to be old status in memory and in db
				req.Process.Tenant.Status = sProcess.Tenant.Status
				copy.Status = sProcess.Tenant.Status
				break
			}
		}
		delete(m.ShardProcesses, req.Process.Tenant.Name)
	} else {
		found := false
		for _, sProcess := range m.ShardProcesses[req.Process.Tenant.Name] {
			if sProcess.Op == command.TenantsProcess_OP_DONE && req.Process.Tenant.Status == types.TenantActivityStatusUNFROZEN {
				copy.Status = req.Process.Tenant.Status
				continue
			}

			if sProcess.Op == command.TenantsProcess_OP_ABORT && req.Process.Tenant.Status != types.TenantActivityStatusUNFROZEN {
				found = true
				// force tenant status to be old status in memory and in db
				req.Process.Tenant.Status = sProcess.Tenant.Status
				copy.Status = sProcess.Tenant.Status
				delete(m.ShardProcesses, req.Process.Tenant.Name)
				break
			}
		}
		if !found {
			req.Process = nil
			return nil
		}
	}

	m.ShardVersion = v
	m.Sharding.Physical[copy.Name] = copy
	if !slices.Contains(copy.BelongsToNodes, nodeID) {
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
		if p.ActivityStatus() == u.Status {
			req.Tenants[i] = nil
			continue
		}

		if u.Status != models.TenantActivityStatusFROZEN && p.ActivityStatus() == types.TenantActivityStatusFREEZING {
			return n, fmt.Errorf("there is freezing in progress")
		}
		if u.Status != models.TenantActivityStatusFROZEN && p.ActivityStatus() == types.TenantActivityStatusUNFREEZING {
			return n, fmt.Errorf("there is unfreezing in progress")
		}
		if len(m.ShardProcesses) > 0 {
			if _, exists := m.ShardProcesses[u.Name]; exists {
				return n, fmt.Errorf("there is process in progress")
			}
		}

		// means it's either hot or cold
		if u.Status != models.TenantActivityStatusFROZEN && p.ActivityStatus() == models.TenantActivityStatusFROZEN {
			// TODO: requested will be either hot or cold we need to know
			if len(m.ShardProcesses) == 0 {
				m.ShardProcesses = make(map[string]NodesCloudStatus)
			}

			process, ok := m.ShardProcesses[u.Name]
			if !ok {
				process = make(map[string]*api.TenantsProcess)
			}

			partitions, err := m.Sharding.GetPartitions(req.ClusterNodes, []string{u.Name}, m.Class.ReplicationConfig.Factor)
			if err != nil {
				return n, fmt.Errorf("get partitions: %w", err)
			}
			// TODO-RAFT: Check in which cases can the partition not have assigned one to a tenant
			// fmt.Println("new nodes")
			newNodes, ok := partitions[u.Name]
			if !ok {
				// TODO-RAFT: Do we want to silently continue here or raise an error ?
				continue
			}

			oldNodes := p.BelongsToNodes

			// TODO here the details for download and don't replace the sharding state
			// p = sharding.Physical{Name: u.Name, Status: types.TenantActivityStatusUNFREEZING, BelongsToNodes: newNodes}
			p.Status = types.TenantActivityStatusUNFREEZING
			p.BelongsToNodes = newNodes
			m.Sharding.Physical[u.Name] = p

			// TODO what of oldNodes
			// <
			// > delete cloud and also has to be  done on replication factor change
			newToOld := map[string]string{}
			slices.Sort(newNodes)
			slices.Sort(oldNodes)
			for idx, node := range newNodes {
				newToOld[node] = oldNodes[idx]

				process[node] = &api.TenantsProcess{
					Op: command.TenantsProcess_OP_START,
					Tenant: &command.Tenant{
						Status: u.Status,
					},
				}
			}

			if _, exists := newToOld[nodeID]; !exists {
				// it does not belong to the new partitions
				req.Tenants[i] = nil
				continue
			}
			m.ShardProcesses[u.Name] = process
			req.Tenants[i].Name = fmt.Sprintf("%s#%s", req.Tenants[i].Name, newToOld[nodeID])
			req.Tenants[i].Status = p.Status

		}

		if u.Status == models.TenantActivityStatusFROZEN && p.ActivityStatus() != types.TenantActivityStatusFREEZING {
			u.Status = types.TenantActivityStatusFREEZING
			if len(m.ShardProcesses) == 0 {
				m.ShardProcesses = make(map[string]NodesCloudStatus)
			}

			process, ok := m.ShardProcesses[u.Name]
			if !ok {
				process = make(map[string]*api.TenantsProcess)
			}

			for _, node := range p.BelongsToNodes {
				process[node] = &api.TenantsProcess{
					Op: command.TenantsProcess_OP_START,
				}
			}

			// update request
			req.Tenants[i] = u
			m.ShardProcesses[u.Name] = process
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
