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
		return mc, v
	}
	m.RLock()
	defer m.RUnlock()
	if m.Class.MultiTenancyConfig == nil {
		return mc, v
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
	nodes := x.RoutingNodes()
	if len(nodes) < 1 || nodes[0] == "" {
		return "", 0, fmt.Errorf("owner node not found")
	}

	// we randomize the owner if there is more than one node
	// - avoid hotspots
	// - tolerate down nodes
	// - distribute load
	return nodes[rand.Intn(len(nodes))], m.version(), nil
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
	return slices.Clone(x.RoutingNodes()), m.version(), nil
}

// TenantsShardsStatus returns shard name for the provided tenant and its activity status
func (m *metaClass) TenantsShardsStatus(class string, tenants ...string) (map[string]string, uint64) {
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

func (m *metaClass) AddProperty(v uint64, props ...*models.Property) error {
	return m.upsertProperty(v, nil, props...)
}

// UpdateProperty merges `property` into the existing class. When `mask` is
// non-empty, only the listed property fields are merged; the rest are kept
// from the existing class state. An empty / nil mask preserves the legacy
// "merge everything" semantics.
//
// Returns the merged property (a *new* pointer, not aliased with anything
// stored in the class) so the caller can pass the post-merge view to the
// storage layer, which needs the full property to decide which buckets to
// create or remove. Returns nil if the property is not found.
func (m *metaClass) UpdateProperty(v uint64, property *models.Property, mask []string) (*models.Property, error) {
	if err := m.upsertProperty(v, mask, property); err != nil {
		return nil, err
	}
	// Look up the merged property (after upsert) and return a copy so the
	// caller can safely use it without holding m's lock.
	m.RLock()
	defer m.RUnlock()
	for _, p := range m.Class.Properties {
		if strings.EqualFold(p.Name, property.Name) {
			merged := *p
			return &merged, nil
		}
	}
	return nil, nil
}

// upsertProperty method takes properties and merges them with existing class
// if property doesn't exist then it will be added
// if property exists then it will be merged with existing property
//
// When `mask` is non-empty, only the listed fields are merged on the update
// path (see PropertyField* constants in cluster/proto/api). The mask only
// applies to existing properties; brand new properties (i.e. on AddProperty)
// are always added in full.
func (m *metaClass) upsertProperty(v uint64, mask []string, props ...*models.Property) error {
	m.Lock()
	defer m.Unlock()

	// update all at once to prevent race condition with concurrent readers
	mergedProps := MergePropsMasked(m.Class.Properties, props, mask)
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
// final property is created by merging new property into copy of old one.
//
// Equivalent to MergePropsMasked with a nil mask: all supported fields are
// considered for the merge.
func MergeProps(old, new []*models.Property) []*models.Property {
	return MergePropsMasked(old, new, nil)
}

// MergePropsMasked is the masked variant of MergeProps. When mask is empty
// or nil, every supported field is considered for the merge (legacy
// behavior). When mask is non-empty, only the listed fields are merged onto
// existing properties; the rest are taken from the old slice.
//
// New properties (names not present in old) are always inserted in full —
// the mask only restricts which fields are *overwritten* on existing
// properties. This matches the AddProperty / UpdateProperty split: the
// add path uses MergeProps with no mask, while the update path can supply
// a mask to avoid clobbering fields the caller doesn't own.
//
// Concurrency context: without a mask, two concurrent UpdateProperty calls
// on the same property race because each one reads the schema, then issues
// a full-property replace. A mask scopes each call to only the fields the
// caller actually modified, so two disjoint callers (e.g. one flipping
// IndexFilterable, one flipping IndexSearchable) can both succeed without
// stepping on each other.
func MergePropsMasked(old, new []*models.Property, mask []string) []*models.Property {
	mergedProps := make([]*models.Property, len(old), len(old)+len(new))
	copy(mergedProps, old)

	// create memory to avoid duplication
	mem := make(map[string]int, len(old))
	for idx := range old {
		mem[strings.ToLower(old[idx].Name)] = idx
	}

	// Build a quick-lookup set for the mask. A nil/empty mask means "every
	// field allowed" — we encode that as a nil set and check below.
	var allowed map[string]struct{}
	if len(mask) > 0 {
		allowed = make(map[string]struct{}, len(mask))
		for _, f := range mask {
			allowed[f] = struct{}{}
		}
	}
	allow := func(field string) bool {
		if allowed == nil {
			return true
		}
		_, ok := allowed[field]
		return ok
	}

	// pick ones not present in old slice or merge nested properties
	// if already present
	for idx := range new {
		oldIdx, exists := mem[strings.ToLower(new[idx].Name)]
		if !exists {
			mergedProps = append(mergedProps, new[idx])
			continue
		}

		if allow(command.PropertyFieldIndexRangeFilters) {
			mergedProps[oldIdx].IndexRangeFilters = new[idx].IndexRangeFilters
		}
		if allow(command.PropertyFieldIndexFilterable) {
			mergedProps[oldIdx].IndexFilterable = new[idx].IndexFilterable
		}
		if allow(command.PropertyFieldIndexSearchable) {
			mergedProps[oldIdx].IndexSearchable = new[idx].IndexSearchable
		}
		if allow(command.PropertyFieldTokenization) && new[idx].Tokenization != "" {
			mergedProps[oldIdx].Tokenization = new[idx].Tokenization
		}
		if allow(command.PropertyFieldBucketGeneration) {
			// BucketGeneration is monotonically increasing; semantic
			// runtime-reindex migrations bump it atomically with the
			// schema-flag flip. Replace unconditionally — callers always
			// submit the next-generation value when the field is in the
			// mask.
			mergedProps[oldIdx].BucketGeneration = new[idx].BucketGeneration
		}
		if allow(command.PropertyFieldSearchableBlockmax) {
			mergedProps[oldIdx].SearchableBlockmax = new[idx].SearchableBlockmax
		}

		if allow(command.PropertyFieldNestedProperties) {
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

// UpdateTenantsProcess leaves in req only the reports this node's DB has to act on.
func (m *metaClass) UpdateTenantsProcess(nodeID string, req *command.TenantProcessRequest, v uint64) (map[string]int, error) {
	m.Lock()
	defer m.Unlock()

	// sc tracks number of tenants updated by "status"
	sc := make(map[string]int)

	for idx, report := range req.TenantsProcesses {
		if report == nil || report.Tenant == nil {
			req.TenantsProcesses[idx] = nil
			continue
		}
		shard, ok := m.Sharding.Physical[report.Tenant.Name]
		if !ok || !reportApplies(shard.Transition, req, report) {
			req.TenantsProcesses[idx] = nil
			continue
		}

		updated := shard.DeepCopy()
		settleReport(&updated, req.Node, report)
		m.Sharding.Physical[updated.Name] = updated
		m.ShardVersion = v

		if updated.Transition != nil {
			req.TenantsProcesses[idx] = nil
			continue
		}
		sc[shard.CountedStatus()]--
		sc[updated.CountedStatus()]++

		if !actsOnEnd(shard, updated, nodeID) {
			req.TenantsProcesses[idx] = nil
			continue
		}
		report.Tenant.Status = updated.Status
	}
	return sc, nil
}

// actsOnEnd includes an aborted unfreeze's participants, to clear what they may have swapped in.
func actsOnEnd(before, after sharding.Physical, nodeID string) bool {
	if before.Transition.Kind == sharding.TransitionUnfreeze && after.Status == models.TenantActivityStatusFROZEN {
		_, ok := before.Transition.Participants[nodeID]
		return ok
	}
	return slices.Contains(after.BelongsToNodes, nodeID)
}

// reportApplies accepts only DONE or ABORT, as settleReport counts anything else as done.
func reportApplies(t *sharding.Transition, req *command.TenantProcessRequest, report *command.TenantsProcess) bool {
	if report.Op != command.TenantsProcess_OP_DONE && report.Op != command.TenantsProcess_OP_ABORT {
		return false
	}
	if t == nil || t.Kind != transitionKind(req.Action) || report.Attempt != t.Attempt {
		return false
	}
	participant, ok := t.Participants[req.Node]
	return ok && participant.Progress == sharding.ProgressStarted
}

func transitionKind(action command.TenantProcessRequest_Action) string {
	if action == command.TenantProcessRequest_ACTION_FREEZING {
		return sharding.TransitionFreeze
	}
	return sharding.TransitionUnfreeze
}

func settleReport(p *sharding.Physical, node string, report *command.TenantsProcess) {
	t := p.Transition
	if report.Op == command.TenantsProcess_OP_ABORT {
		p.Transition = nil
		return
	}

	participant := t.Participants[node]
	participant.Progress = sharding.ProgressDone
	participant.HasData = report.HasData
	t.Participants[node] = participant
	if !t.AllDone() {
		return
	}

	p.Transition = nil
	if t.Kind == sharding.TransitionFreeze {
		p.Status = models.TenantActivityStatusFROZEN
		p.CloudAttempt = t.Attempt
		p.CloudSources = t.DataNodes()
		return
	}
	p.Status = t.Target
	p.BelongsToNodes = t.Chosen
	p.CloudAttempt = 0
	p.CloudSources = nil
}

// UpdateTenants applies the requested tenant statuses; a freeze or unfreeze starts a transition at v.
func (m *metaClass) UpdateTenants(nodeID string, req *command.UpdateTenantsRequest, replicationFSM replicationFSM, v uint64) (map[string]int, error) {
	m.Lock()
	defer m.Unlock()

	// sc tracks number of tenants updated by "status"
	sc := make(map[string]int)

	// For each requested tenant update we'll check if we the schema is missing that shard. If we have any missing shard
	// we'll return an error but any other successful shard will be updated.
	// If we're not adding a new shard we'll then check if the activity status needs to be changed
	// If the activity status is changed we will deep copy the tenant and update the status
	var partialErrs []error
	writeIndex := 0

	for i, requestTenant := range req.Tenants {
		oldTenant, ok := m.Sharding.Physical[requestTenant.Name]
		// If we can't find the shard add it to missing shards to error later
		if !ok {
			partialErrs = append(partialErrs, fmt.Errorf("%w: %s", ErrShardNotFound, requestTenant.Name))
			continue
		}
		oldStatus := oldTenant.ActivityStatus()

		// validate status
		switch oldStatus {
		case req.Tenants[i].Status:
			continue
		case types.TenantActivityStatusFREEZING:
			// Allow the in-flight freeze to complete (FROZEN is the expected terminal state).
			// Reject any other status change (e.g. HOT/COLD) while freeze is in progress to
			// prevent a race where UNFREEZE is triggered before the FREEZE goroutine finishes,
			// which can lead to permanent data loss.
			if requestTenant.Status == models.TenantActivityStatusFROZEN {
				continue
			}
			partialErrs = append(partialErrs, fmt.Errorf("%w: tenant %q is currently being frozen, cannot change status to %s",
				ErrTenantTransitionalState, requestTenant.Name, requestTenant.Status))
			continue
		case types.TenantActivityStatusUNFREEZING:
			// Allow requests that match the status the ongoing unfreeze is targeting.
			// Reject any conflicting status change while unfreeze is in progress.
			req.ImplicitUpdateRequest = true
			var statusInProgress string
			if oldTenant.Transition != nil {
				statusInProgress = oldTenant.Transition.Target
			}
			if requestTenant.Status == statusInProgress {
				continue
			}
			partialErrs = append(partialErrs, fmt.Errorf("%w: tenant %q is currently being unfrozen to %s, cannot change status to %s",
				ErrTenantTransitionalState, requestTenant.Name, statusInProgress, requestTenant.Status))
			continue
		}

		isFrozen := oldStatus == models.TenantActivityStatusFROZEN
		requestedToFrozen := requestTenant.Status == models.TenantActivityStatusFROZEN

		// COLD shuts the shard down, freeze offloads it, unfreeze reassigns its nodes — each
		// breaks an in-flight movement. (oldTenant is never FREEZING/UNFREEZING here.)
		toCold := requestTenant.Status == models.TenantActivityStatusCOLD
		toFrozen := requestedToFrozen && !isFrozen
		fromFrozen := isFrozen && !requestedToFrozen
		if (toCold || toFrozen || fromFrozen) && replicationFSM != nil && replicationFSM.HasActiveReplicationForShard(m.Class.Class, requestTenant.Name) {
			partialErrs = append(partialErrs, fmt.Errorf(
				"%w: tenant %q status change to %s blocked; retry after movement completes",
				ErrReplicaMovementInProgress, requestTenant.Name, requestTenant.Status))
			continue
		}

		newTenant := oldTenant.DeepCopy()
		involved := slices.Contains(oldTenant.BelongsToNodes, nodeID)

		switch {
		case fromFrozen:
			transition, err := m.unfreezeTransition(req, &oldTenant, requestTenant.Status, v)
			if err != nil {
				partialErrs = append(partialErrs, fmt.Errorf("tenant %q: %w", requestTenant.Name, err))
				continue
			}
			newTenant.Transition = transition
			_, involved = transition.Participants[nodeID]
			requestTenant.Status = types.TenantActivityStatusUNFREEZING
		case toFrozen:
			newTenant.Transition = freezeTransition(&oldTenant, v)
			requestTenant.Status = types.TenantActivityStatusFREEZING
		default:
			newTenant.Status = requestTenant.Status
		}

		// Update the schema tenant representation with the deep copy (necessary as the initial is a shallow copy from
		// the map read
		m.Sharding.Physical[oldTenant.Name] = newTenant

		// At this point we know, we are going to change the status of a tenant from old-state to new-state.
		sc[oldTenant.CountedStatus()]--
		sc[newTenant.CountedStatus()]++

		if !involved {
			continue
		}

		// Save the "valid" tenant on writeIndex and increment. This allows us to filter in place in req.Tenants the
		// tenants that actually have a change to process
		req.Tenants[writeIndex] = requestTenant
		writeIndex++
	}

	// Remove the ignore tenants from the request to act as filter on the subsequent DB update
	req.Tenants = req.Tenants[:writeIndex]

	// Wrap any partial errors (missing shards, transitional-state conflicts) so
	// apply() knows to still call updateStore() for the tenants that succeeded.
	var err error
	if len(partialErrs) > 0 {
		err = &PartialUpdateError{Errs: partialErrs}
	}
	// Update the version of the shard to the current version
	m.ShardVersion = v

	return sc, err
}

func freezeTransition(p *sharding.Physical, v uint64) *sharding.Transition {
	participants := make(map[string]sharding.TransitionParticipant, len(p.BelongsToNodes))
	for _, node := range p.BelongsToNodes {
		participants[node] = sharding.TransitionParticipant{Progress: sharding.ProgressStarted}
	}
	return &sharding.Transition{
		Kind:         sharding.TransitionFreeze,
		Target:       models.TenantActivityStatusFROZEN,
		Attempt:      v,
		Participants: participants,
	}
}

// unfreezeTransition spreads the chosen nodes over the copies that hold data.
func (m *metaClass) unfreezeTransition(req *command.UpdateTenantsRequest, p *sharding.Physical, target string, v uint64) (*sharding.Transition, error) {
	partitions, err := m.Sharding.GetPartitions(req.ClusterNodes, []string{p.Name}, m.Class.ReplicationConfig.Factor)
	if err != nil {
		return nil, fmt.Errorf("get partitions: %w", err)
	}
	chosen, ok := partitions[p.Name]
	if !ok {
		return nil, fmt.Errorf("can not assign new nodes to shard %s, it didn't exist in the new partitions", p.Name)
	}

	slices.Sort(chosen)
	participants := make(map[string]sharding.TransitionParticipant, len(chosen))
	for idx, node := range chosen {
		participant := sharding.TransitionParticipant{Progress: sharding.ProgressStarted}
		if len(p.CloudSources) > 0 {
			participant.Source = p.CloudSources[idx%len(p.CloudSources)]
		}
		participants[node] = participant
	}
	return &sharding.Transition{
		Kind:         sharding.TransitionUnfreeze,
		Target:       target,
		Attempt:      v,
		Participants: participants,
		Chosen:       chosen,
	}, nil
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
