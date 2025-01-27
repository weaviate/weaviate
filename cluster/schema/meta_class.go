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

func (m *metaClass) AddTenants(nodeID string, req *command.AddTenantsRequest, replFactor int64, v uint64) (map[string]int, error) {
	req.Tenants = removeNilTenants(req.Tenants)
	m.Lock()
	defer m.Unlock()

	sc := make(map[string]int)

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
		if status, ok := m.Sharding.DeletePartition(name); ok {
			count[status]++
		}
	}
	m.ShardVersion = v
	return count, nil
}

func (m *metaClass) UpdateTenants(nodeID string, req *command.UpdateTenantsRequest, v uint64) (map[string]int, error) {
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
	for _, requestTenant := range req.Tenants {
		oldTenant, ok := m.Sharding.Physical[requestTenant.Name]
		// If we can't find the shard add it to missing shards to error later
		if !ok {
			missingShards = append(missingShards, requestTenant.Name)
			continue
		}
		// If the status is currently the same as the one requested just ignore
		if oldTenant.ActivityStatus() == requestTenant.Status {
			continue
		}

		newTenant := oldTenant.DeepCopy()
		newTenant.Status = requestTenant.Status
		// Update the schema tenant representation with the deep copy (necessary as the initial is a shallow copy from
		// the map read
		m.Sharding.Physical[oldTenant.Name] = newTenant

		// If the shard is not stored on that node skip updating the request tenant as there will be nothing to load on
		// the DB side
		if !slices.Contains(oldTenant.BelongsToNodes, nodeID) {
			continue
		}

		// At this point we know, we are going to change the status of a tenant from old-state to new-state.
		// TODO: what happes if it's stored on `m.Sharding.Physical` but returned if nodeID not present on `oldTenant.BelongsToNodes`?
		sc[oldTenant.ActivityStatus()]--
		sc[newTenant.ActivityStatus()]++

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
