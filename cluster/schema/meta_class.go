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
	"math/rand"
	"slices"
	"strings"
	"sync"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/models"
	entSchema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type (
	NodeShardProcess map[string]*command.TenantsProcess
	metaClass        struct {
		sync.RWMutex
		Class        models.Class
		ClassVersion uint64
		Sharding     sharding.State
		ShardVersion uint64
		// ShardProcesses map[tenantName-action(FREEZING/UNFREEZING)]map[nodeID]TenantsProcess
		ShardProcesses map[string]NodeShardProcess
	}
)

func (m *metaClass) ClassInfo() ClassInfo {
	if m == nil {
		return ClassInfo{}
	}

	m.RLock()
	defer m.RUnlock()

	ci := ClassInfo{
		ReplicationFactor: 1,
		Exists:            true,
		Properties:        len(m.Class.Properties),
		MultiTenancy:      models.MultiTenancyConfig{},
		Tenants:           len(m.Sharding.Physical),
		ClassVersion:      m.ClassVersion,
		ShardVersion:      m.ShardVersion,
	}

	if m.Class.MultiTenancyConfig != nil {
		ci.MultiTenancy = *m.Class.MultiTenancyConfig
	}
	if m.Class.ReplicationConfig != nil && m.Class.ReplicationConfig.Factor > 1 {
		ci.ReplicationFactor = int(m.Class.ReplicationConfig.Factor)
	}
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
// will randomize the owner if there is more than one node
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

	// we randomize the owner if there is more than one node
	// - avoid hotspots
	// - tolerate down nodes
	// - distribute load
	return x.BelongsToNodes[rand.Intn(len(x.BelongsToNodes))], m.version(), nil
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

func (m *metaClass) AddReplicaToShard(v uint64, shard string, replica string) error {
	m.Lock()
	defer m.Unlock()

	err := m.Sharding.AddReplicaToShard(shard, replica)
	if err != nil {
		return err
	}
	m.ClassVersion = v
	return nil
}

func (m *metaClass) DeleteReplicaFromShard(v uint64, shard string, replica string) error {
	m.Lock()
	defer m.Unlock()

	err := m.Sharding.DeleteReplicaFromShard(shard, replica)
	if err != nil {
		return err
	}
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
			mergedProps[oldIdx].IndexRangeFilters = new[idx].IndexRangeFilters

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

func (m *metaClass) AddTenants(nodeID string, req *command.AddTenantsRequest, replFactor int64, v uint64) (map[string]int, error) {
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
		return nil, fmt.Errorf("get partitions: %w", err)
	}

	// sc tracks number of shards in this collection to be added by status.
	sc := make(map[string]int)

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
		if m.Sharding.Physical == nil {
			m.Sharding.Physical = make(map[string]sharding.Physical, 128)
		}
		m.Sharding.Physical[t.Name] = p
		// TODO-RAFT: Check here why we set =nil if it is "owned by another node"
		if !slices.Contains(part, nodeID) {
			req.Tenants[i] = nil // is owned by another node
		}
		sc[p.Status]++
	}
	m.ShardVersion = v
	req.Tenants = removeNilTenants(req.Tenants)
	return sc, nil
}

// DeleteTenants try to delete the tenants from given request and returns
// total number of deleted tenants.
func (m *metaClass) DeleteTenants(req *command.DeleteTenantsRequest, v uint64) (map[string]int, error) {
	m.Lock()
	defer m.Unlock()

	count := make(map[string]int)

	for _, name := range req.Tenants {
		shardingState := m.Sharding
		status, ok, err := shardingState.DeletePartition(name)
		if err != nil {
			return nil, fmt.Errorf("error while migrating sharding state: %w", err)
		}
		if ok {
			count[status]++
		}
	}
	m.ShardVersion = v
	return count, nil
}

func (m *metaClass) UpdateTenantsProcess(nodeID string, req *command.TenantProcessRequest, v uint64) (map[string]int, error) {
	m.Lock()
	defer m.Unlock()

	// sc tracks number of tenants updated by "status"
	sc := make(map[string]int)

	for idx := range req.TenantsProcesses {
		name := req.TenantsProcesses[idx].Tenant.Name

		shard, ok := m.Sharding.Physical[name]
		if !ok {
			return nil, fmt.Errorf("shard %s not found", name)
		}
		oldStatus := shard.Status

		if req.Action == command.TenantProcessRequest_ACTION_UNFREEZING {
			// on unfreezing get the requested status from the shard process
			if status := m.findRequestedStatus(nodeID, name, req.Action); status != "" {
				req.TenantsProcesses[idx].Tenant.Status = status
			}
		}

		// NOTE: Have to get the `newStatus` only after `findRequestedStatus`, else req.Tenant.Status can be empty.
		newStatus := req.TenantsProcesses[idx].Tenant.Status

		process := m.shardProcess(name, req.Action)
		process[req.Node] = req.TenantsProcesses[idx]

		if m.allShardProcessExecuted(name, req.Action) {
			m.applyShardProcess(name, req.Action, req.TenantsProcesses[idx], &shard)
		} else {
			// ignore applying in case of aborts (upload action only)
			if !m.updateShardProcess(name, req.Action, req.TenantsProcesses[idx], &shard) {
				req.TenantsProcesses[idx] = nil
				continue
			}
		}

		m.ShardVersion = v
		m.Sharding.Physical[shard.Name] = shard

		sc[oldStatus]--
		sc[newStatus]++

		if !slices.Contains(shard.BelongsToNodes, nodeID) {
			req.TenantsProcesses[idx] = nil
			continue
		}
	}
	return sc, nil
}

func (m *metaClass) UpdateTenants(nodeID string, req *command.UpdateTenantsRequest, replicationFSM replicationFSM, v uint64) (map[string]int, error) {
	m.Lock()
	defer m.Unlock()

	// sc tracks number of tenants updated by "status"
	sc := make(map[string]int)

	// For each requested tenant update we'll check if we the schema is missing that shard. If we have any missing shard
	// we'll return an error but any other successful shard will be updated.
	// If we're not adding a new shard we'll then check if the activity status needs to be changed
	// If the activity status is changed we will deep copy the tenant and update the status
	missingShards := []string{}
	writeIndex := 0

	for i, requestTenant := range req.Tenants {
		oldTenant, ok := m.Sharding.Physical[requestTenant.Name]
		oldStatus := oldTenant.Status
		// If we can't find the shard add it to missing shards to error later
		if !ok {
			missingShards = append(missingShards, requestTenant.Name)
			continue
		}

		// validate status
		switch oldTenant.ActivityStatus() {
		case req.Tenants[i].Status:
			continue
		case types.TenantActivityStatusFREEZING:
			// ignore multiple freezing
			if requestTenant.Status == models.TenantActivityStatusFROZEN {
				continue
			}
		case types.TenantActivityStatusUNFREEZING:
			// ignore multiple unfreezing
			var statusInProgress string
			processes, exists := m.ShardProcesses[shardProcessID(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_UNFREEZING)]
			if exists {
				for _, process := range processes {
					statusInProgress = process.Tenant.Status
					break
				}
			}
			if requestTenant.Status == statusInProgress {
				continue
			}
		}

		if requestTenant.Status == models.TenantActivityStatusCOLD && replicationFSM.HasOngoingReplication(m.Class.Class, requestTenant.Name, nodeID) {
			continue
		}

		existedSharedFrozen := oldTenant.ActivityStatus() == models.TenantActivityStatusFROZEN || oldTenant.ActivityStatus() == models.TenantActivityStatusFREEZING
		requestedToFrozen := requestTenant.Status == models.TenantActivityStatusFROZEN

		switch {
		case existedSharedFrozen && !requestedToFrozen:
			if err := m.unfreeze(nodeID, i, req, &oldTenant); err != nil {
				return sc, err
			}
			if req.Tenants[i] != nil {
				requestTenant.Status = req.Tenants[i].Status
			}

		case requestedToFrozen && !existedSharedFrozen:
			m.freeze(i, req, oldTenant)
		default:
			// do nothing
		}

		newTenant := oldTenant.DeepCopy()
		newTenant.Status = requestTenant.Status

		// Update the schema tenant representation with the deep copy (necessary as the initial is a shallow copy from
		// the map read
		m.Sharding.Physical[oldTenant.Name] = newTenant

		// At this point we know, we are going to change the status of a tenant from old-state to new-state.
		sc[oldStatus]--
		sc[newTenant.ActivityStatus()]++

		// If the shard is not stored on that node skip updating the request tenant as there will be nothing to load on
		// the DB side
		if !slices.Contains(oldTenant.BelongsToNodes, nodeID) {
			continue
		}

		// Save the "valid" tenant on writeIndex and increment. This allows us to filter in place in req.Tenants the
		// tenants that actually have a change to process
		req.Tenants[writeIndex] = requestTenant
		writeIndex++
	}

	// Remove the ignore tenants from the request to act as filter on the subsequent DB update
	req.Tenants = req.Tenants[:writeIndex]

	// Check for any missing shard to return an error
	var err error
	if len(missingShards) > 0 {
		err = fmt.Errorf("%w: %v", ErrShardNotFound, missingShards)
	}
	// Update the version of the shard to the current version
	m.ShardVersion = v

	return sc, err
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

func (m *metaClass) updateShardProcess(name string, action command.TenantProcessRequest_Action, req *command.TenantsProcess, copy *sharding.Physical) bool {
	processes := m.ShardProcesses[shardProcessID(name, action)]
	switch action {
	case command.TenantProcessRequest_ACTION_UNFREEZING:
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_DONE {
				copy.Status = sp.Tenant.Status
				req.Tenant.Status = sp.Tenant.Status
				break
			}
		}
		return false
	case command.TenantProcessRequest_ACTION_FREEZING:
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_ABORT {
				copy.Status = req.Tenant.Status
				req.Tenant.Status = sp.Tenant.Status
				return true
			}
		}
	default:
		return false
	}

	return false
}

func (m *metaClass) applyShardProcess(name string, action command.TenantProcessRequest_Action, req *command.TenantsProcess, copy *sharding.Physical) {
	processes := m.ShardProcesses[shardProcessID(name, action)]
	switch action {
	case command.TenantProcessRequest_ACTION_UNFREEZING:
		for _, sp := range processes {
			if sp.Op == command.TenantsProcess_OP_DONE {
				copy.Status = sp.Tenant.Status
				req.Tenant.Status = sp.Tenant.Status
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
			copy.Status = req.Tenant.Status
		} else {
			copy.Status = onAbortStatus
			req.Tenant.Status = onAbortStatus
		}
	default:
		// do nothing
		return
	}
	delete(m.ShardProcesses, shardProcessID(name, action))
}

func (m *metaClass) shardProcess(name string, action command.TenantProcessRequest_Action) map[string]*command.TenantsProcess {
	if len(m.ShardProcesses) == 0 {
		m.ShardProcesses = make(map[string]NodeShardProcess)
	}

	process, ok := m.ShardProcesses[shardProcessID(name, action)]
	if !ok {
		process = make(map[string]*command.TenantsProcess)
	}
	return process
}

// freeze creates a process requests and add them in memory to compare it later when
// TenantProcessRequest comes.
// it updates the tenant status to FREEZING in RAFT schema
func (m *metaClass) freeze(i int, req *command.UpdateTenantsRequest, shard sharding.Physical) {
	process := m.shardProcess(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_FREEZING)

	for _, node := range shard.BelongsToNodes {
		process[node] = &command.TenantsProcess{
			Op: command.TenantsProcess_OP_START,
			Tenant: &command.Tenant{
				Name:   req.Tenants[i].Name,
				Status: req.Tenants[i].Status,
			},
		}
	}
	// to be proceed in the db layer
	req.Tenants[i].Status = types.TenantActivityStatusFREEZING
	m.ShardProcesses[shardProcessID(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_FREEZING)] = process
}

// unfreeze creates a process requests and add them in memory to compare it later when
// TenantProcessRequest comes.
// it keeps the requested state ACTIVE/INACTIVE in memory.
// it updates the tenant status to UNFREEZING in RAFT schema.
// NOTE: can make some of the requests nil.
func (m *metaClass) unfreeze(nodeID string, i int, req *command.UpdateTenantsRequest, p *sharding.Physical) error {
	name := req.Tenants[i].Name
	process := m.shardProcess(name, command.TenantProcessRequest_ACTION_UNFREEZING)

	partitions, err := m.Sharding.GetPartitions(req.ClusterNodes, []string{name}, m.Class.ReplicationConfig.Factor)
	if err != nil {
		req.Tenants[i] = nil
		return fmt.Errorf("get partitions: %w", err)
	}

	newNodes, ok := partitions[name]
	if !ok {
		req.Tenants[i] = nil
		return fmt.Errorf("can not assign new nodes to shard %s, it didn't exist in the new partitions", name)
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
		process[node] = &command.TenantsProcess{
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
		return nil
	}
	m.ShardProcesses[shardProcessID(req.Tenants[i].Name, command.TenantProcessRequest_ACTION_UNFREEZING)] = process
	req.Tenants[i].Name = fmt.Sprintf("%s#%s", req.Tenants[i].Name, newToOld[nodeID])
	req.Tenants[i].Status = p.Status
	return nil
}
