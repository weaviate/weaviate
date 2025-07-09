//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package router provides an abstraction for determining the optimal routing plans
// for reads and writes within a Weaviate cluster. It handles logic around sharding,
// replication, and consistency, helping determine which nodes and shards (replicas)
// should be queried for a given operation.
//
// The Router interface is implemented by single-tenant and multi-tenant routers,
// depending on the system's configuration. Use NewBuilder to create the
// appropriate router based on whether partitioning is enabled.
package router

import (
	"context"
	"fmt"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/usecases/schema"

	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Builder provides a builder for creating router instances based on configuration.
// Use NewBuilder() with all required parameters, then call Build() to get the appropriate Router implementation,
// either a multi-tenant router or a single tenant router. The multi-tenant router will use the tenant name as the
// partitioning key to identify a specific tenant's partitioning.
type Builder struct {
	collection           string
	partitioningEnabled  bool
	nodeSelector         cluster.NodeSelector
	schemaGetter         schema.SchemaGetter
	schemaReader         schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
}

// NewBuilder creates a new Builder with the provided configuration.
//
// Parameters:
//   - collection: the name of the collection that this router will handle.
//   - partitioningEnabled: true for multi-tenant mode, false for single-tenant mode.
//   - nodeSelector: provides cluster node state information and hostnames.
//   - schemaGetter: provides collection schemas, sharding states, and tenant information.
//   - schemaReader: provides shard replica (or node names) metadata.
//   - replicationFSMReader: provides replica state information for replication consistency.
//
// Returns:
//   - *Builder: a new builder instance ready to build the appropriate router.
func NewBuilder(
	collection string,
	partitioningEnabled bool,
	nodeSelector cluster.NodeSelector,
	schemaGetter schema.SchemaGetter,
	schemaReader schemaTypes.SchemaReader,
	replicationFSMReader replicationTypes.ReplicationFSMReader,
) *Builder {
	return &Builder{
		collection:           collection,
		partitioningEnabled:  partitioningEnabled,
		nodeSelector:         nodeSelector,
		schemaGetter:         schemaGetter,
		schemaReader:         schemaReader,
		replicationFSMReader: replicationFSMReader,
	}
}

// Build builds and returns the appropriate router implementation based on the partitioning configuration.
//
// Returns:
//   - Router: a concrete router implementation (*multiTenantRouter or *singleTenantRouter) that implements the Router interface.
func (b *Builder) Build() types.Router {
	if b.partitioningEnabled {
		return &multiTenantRouter{
			collection:           b.collection,
			schemaGetter:         b.schemaGetter,
			schemaReader:         b.schemaReader,
			replicationFSMReader: b.replicationFSMReader,
			nodeSelector:         b.nodeSelector,
		}
	}
	return &singleTenantRouter{
		collection:           b.collection,
		schemaReader:         b.schemaReader,
		replicationFSMReader: b.replicationFSMReader,
		nodeSelector:         b.nodeSelector,
	}
}

// multiTenantRouter is the implementation of Router for multi-tenant collections.
// In multi-tenant mode, tenant isolation is achieved through partitioning using
// the tenant name as the partitioning key. Each tenant effectively becomes its own shard.
type multiTenantRouter struct {
	collection           string
	schemaGetter         schema.SchemaGetter
	schemaReader         schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	nodeSelector         cluster.NodeSelector
}

// singleTenantRouter is the implementation of Router for single-tenant collections.
// In single-tenant mode, data is distributed across multiple physical shards without
// tenant-based partitioning. All data belongs to a single logical tenant (empty tenant
// or no partitioning key).
type singleTenantRouter struct {
	collection           string
	schemaReader         schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	nodeSelector         cluster.NodeSelector
}

// multiTenantRouter is the implementation of Router for multi-tenant collections.
// In multi-tenant mode, tenant isolation is achieved through partitioning using
// the tenant name as the partitioning key. Each tenant effectively becomes its own shard.
type multiTenantRouter struct {
	collection           string
	schemaGetter         schema.SchemaGetter
	schemaReader         schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	nodeSelector         cluster.NodeSelector
}

// Interface compliance check at compile time.
var (
	_ types.Router = (*multiTenantRouter)(nil)
	_ types.Router = (*singleTenantRouter)(nil)
)

// sort orders replicas with the preferred node first, followed by the remaining replicas.
func sort(replicas []types.Replica, preferredNodeName string) []types.Replica {
	if len(replicas) == 0 {
		return replicas
	}

	var orderedReplicas []types.Replica
	var otherReplicas []types.Replica

	for _, replica := range replicas {
		if replica.NodeName == preferredNodeName {
			orderedReplicas = append(orderedReplicas, replica)
		} else {
			otherReplicas = append(otherReplicas, replica)
		}
	}

	return append(orderedReplicas, otherReplicas...)
}

// preferredNode determines the preferred node for replica ordering by selecting
// the direct candidate if specified, otherwise falling back to the local node.
func preferredNode(directCandidate string, localNodeName string) string {
	if directCandidate != "" {
		return directCandidate
	}
	return localNodeName
}

// buildReplicas constructs a slice of replicas from node names, resolving hostnames
// for each node and filtering out nodes that cannot be resolved.
func buildReplicas(nodeNames []string, shard string, hostnameResolver func(nodeName string) (string, bool)) []types.Replica {
	if len(nodeNames) == 0 {
		return []types.Replica{}
	}

	replicas := make([]types.Replica, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		if hostAddr, ok := hostnameResolver(nodeName); ok {
			replicas = append(replicas, types.Replica{
				NodeName:  nodeName,
				ShardName: shard,
				HostAddr:  hostAddr,
			})
		}
	}
	return replicas
}

// validateTenant for a single-tenant router checks the tenant is empty and returns an error if it is not.
func (r *singleTenantRouter) validateTenant(tenant string) error {
	if tenant != "" {
		return objects.NewErrMultiTenancy(fmt.Errorf("class %s has multi-tenancy disabled, but request was with tenant", r.collection))
	}
	return nil
}

// NodeHostname returns the hostname for the given node name in single-tenant collections.
func (r *singleTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.nodeSelector.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for single-tenant collections.
func (r *singleTenantRouter) AllHostnames() []string {
	return r.nodeSelector.AllHostnames()
}

// GetReadWriteReplicasLocation returns read and write replicas for single-tenant collections.
// In single-tenant mode, this method aggregates replicas from all physical shards of the collection.
func (r *singleTenantRouter) GetReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReadReplicaSet, writeReplicas types.WriteReplicaSet, err error) {
	return r.getReadWriteReplicasLocation(collection, shard)
}

// GetWriteReplicasLocation returns write replicas for single-tenant collections.
func (r *singleTenantRouter) GetWriteReplicasLocation(collection string, shard string) (types.WriteReplicaSet, error) {
	_, writeReplicas, err := r.getReadWriteReplicasLocation(collection, shard)
	if err != nil {
		return types.WriteReplicaSet{}, err
	}
	return writeReplicas, nil
}

// GetReadReplicasLocation returns read replicas for single-tenant collections.
func (r *singleTenantRouter) GetReadReplicasLocation(collection string, tenant string) (types.ReadReplicaSet, error) {
	readReplicas, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return types.ReadReplicaSet{}, err
	}
	return readReplicas, nil
}

func (r *singleTenantRouter) getReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReadReplicaSet, writeReplicas types.WriteReplicaSet, err error,
) {
	targetShards, err := r.targetShards(collection, shard)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while getting target shards for collection %q shard %q: %w", collection, shard, err)
	}

	var replicas []types.Replica

	for _, shardName := range targetShards {
		readReplica, err := r.readReplicasForShard(collection, tenant, shardName)
		if err != nil {
			return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shardName, err)
		}

		replicas = append(replicas, readReplica...)
	}

	return types.ReadReplicaSet{Replicas: allReadReplicas}, types.WriteReplicaSet{Replicas: allWriteReplicas, AdditionalReplicas: allAdditionalWriteReplicas}, nil
}

// getWriteReplicasLocation returns only write replicas for single-tenant collections.
func (r *singleTenantRouter) getWriteReplicasLocation(collection string, tenant string, shard string) (types.WriteReplicaSet, error) {
	targetShards, err := r.targetShards(collection, shard)
	if err != nil {
		return types.WriteReplicaSet{}, err
	}

	var replicas []types.Replica
	var additionalReplicas []types.Replica

	for _, shardName := range targetShards {
		writeReplica, additionalReplica, err := r.writeReplicasForShard(collection, tenant, shardName)
		if err != nil {
			return types.WriteReplicaSet{}, err
		}

		replicas = append(replicas, writeReplica...)
		additionalReplicas = append(additionalReplicas, additionalReplica...)
	}

	return types.WriteReplicaSet{Replicas: replicas, AdditionalReplicas: additionalReplicas}, nil
}

// targetShards returns either all shards or a single one, depending on the value of the shard parameter.
func (r *singleTenantRouter) targetShards(collection, shard string) ([]string, error) {
	shardingState := r.schemaReader.CopyShardingState(collection)
	if shardingState == nil {
		return []string{}, nil
	}

	if shard == "" {
		return shardingState.AllPhysicalShards(), nil
	}

	found := false
	for _, s := range shardingState.AllPhysicalShards() {
		if s == shard {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("error while trying to find shard: %s in collection: %s", shard, collection)
	}
	return []string{shard}, nil
}

// readReplicasForShard gathers only read replicas for one shard.
func (r *singleTenantRouter) readReplicasForShard(collection, tenant, shard string) ([]types.Replica, error) {
	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shard, err)
	}

	readNodeNames := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	return buildReplicas(readNodeNames, shard, r.nodeSelector.NodeHostname), nil
}

// writeReplicasForShard gathers only write and additional write replicas for one shard.
func (r *singleTenantRouter) writeReplicasForShard(collection, tenant, shard string) (write, additional []types.Replica, err error) {
	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shard, err)
	}

	writeNodeNames, additionalWriteNodeNames := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)

	write = buildReplicas(writeNodeNames, shard, r.nodeSelector.NodeHostname)
	additional = buildReplicas(additionalWriteNodeNames, shard, r.nodeSelector.NodeHostname)

	return write, additional, nil
}

// NodeHostname returns the hostname for the given node name in single-tenant collections.
func (r *singleTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.nodeSelector.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for single-tenant collections.
func (r *singleTenantRouter) AllHostnames() []string {
	return r.nodeSelector.AllHostnames()
}

// GetReadWriteReplicasLocation returns read and write replicas for multi-tenant collections.
func (r *multiTenantRouter) GetReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReadReplicaSet, writeReplicas types.WriteReplicaSet, err error) {
	if err := r.validateTenant(shard); err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while validating tenant for collection %q shard %q: %w", collection, shard, err)
	}
}

func (r *multiTenantRouter) getReadWriteReplicasLocation(collection string, shard string) (types.ReadReplicaSet, types.WriteReplicaSet, error) {
	tenantStatus, err := r.schemaGetter.OptimisticTenantStatus(context.TODO(), collection, shard)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while getting optimistic tenant status for tenant %q: %w", shard, err)
	}

	ok, err := r.tenantExistsAndIsActive(tenantStatus, shard)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while checking tenant status for tenant %q: %w", shard, err)
	}
	if !ok {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while retrieving tenant %q: %w", shard, err)
	}

	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shard, err)
	}

	readNodeNames := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	readReplicas := buildReplicas(readNodeNames, shard, r.nodeSelector.NodeHostname)

	writeNodeNames, additionalWriteNodeNames := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)
	writeReplicas := buildReplicas(writeNodeNames, shard, r.nodeSelector.NodeHostname)
	additionalWriteReplicas := buildReplicas(additionalWriteNodeNames, shard, r.nodeSelector.NodeHostname)

	return types.ReadReplicaSet{Replicas: readReplicas}, types.WriteReplicaSet{Replicas: writeReplicas, AdditionalReplicas: additionalWriteReplicas}, nil
}

func buildReplicas(nodeNames []string, shard string, hostnameResolver func(nodeName string) (string, bool)) []types.Replica {
	replicas := make([]types.Replica, 0, len(nodeNames))
	for _, nodeName := range nodeNames {
		if hostAddr, ok := hostnameResolver(nodeName); ok {
			replicas = append(replicas, types.Replica{
				NodeName:  nodeName,
				ShardName: shard,
				HostAddr:  hostAddr,
			})
		}
	}
	return replicas
}

func (r *multiTenantRouter) tenantExistsAndIsActive(tenantStatus map[string]string, tenant string) (bool, error) {
	status, ok := tenantStatus[tenant]
	if !ok {
		return false, fmt.Errorf("error while checking tenant existence: %q", tenant)
	}

	if status != models.TenantActivityStatusHOT {
		return false, fmt.Errorf("error while checking tenant active status: %q", tenant)
	}
	return true, nil
}

// GetWriteReplicasLocation returns write replicas for multi-tenant collections.
func (r *multiTenantRouter) GetWriteReplicasLocation(collection string, tenant string) (types.WriteReplicaSet, error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.WriteReplicaSet{}, fmt.Errorf("error while validating tenant for collection %q tenant %q: %w", collection, tenant, err)
	}
	_, writeReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	return writeReplicas, err
}

// GetReadReplicasLocation returns read replicas for multi-tenant collections.
func (r *multiTenantRouter) GetReadReplicasLocation(collection string, tenant string) (types.ReadReplicaSet, error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.ReadReplicaSet{}, fmt.Errorf("error while validating tenant for collection %q tenant %q: %w", collection, tenant, err)
	}
	readReplicas, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	return readReplicas, err
}

// NodeHostname returns the hostname for the given node name in multi-tenant collections.
func (r *multiTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.nodeSelector.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for multi-tenant collections.
func (r *multiTenantRouter) AllHostnames() []string {
	return r.nodeSelector.AllHostnames()
}

// validateTenant for a multi-tenant router checks the tenant is not empty and returns an error if it is.
func (r *multiTenantRouter) validateTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("tenant is required for multi-tenant collections")
	}
	return nil
}

// BuildWriteRoutingPlan constructs a write routing plan for single-tenant collections.
func (r *singleTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	return r.buildWriteRoutingPlan(params)
}

func (r *singleTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	_, writeReplicas, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while getting read replicas for collection %s shard %s: %w", r.collection, params.Shard, err)
	}

	if len(writeReplicas.Replicas) == 0 {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while checking replica availability for collection %q shard %q", r.collection, params.Shard)
	}

	// Order replicas with direct candidate first
	sortedWriteReplicas := sort(writeReplicas.Replicas, params.DirectCandidateNode, r.nodeSelector.LocalName())

	plan := types.WriteRoutingPlan{
		Shard: params.Shard,
		ReplicaSet: types.WriteReplicaSet{
			Replicas:           sortedWriteReplicas,
			AdditionalReplicas: writeReplicas.AdditionalReplicas,
		},
		ConsistencyLevel: params.ConsistencyLevel,
	}

	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while validating consistency level: %w", err)
	}
	plan.IntConsistencyLevel = cl
	return plan, nil
}

// BuildWriteRoutingPlan constructs a write routing plan for multi-tenant collections.
func (r *multiTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	return r.buildWriteRoutingPlan(params)
}

func (r *multiTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	// Multi-tenant routing requires a specific tenant shard target to enforce isolation of tenants to specific shards.
	// Unlike single-tenant collections, broadcast writes (empty shard) are prohibited to prevent
	// cross-tenant data leakage. Each tenant maps to exactly one shard using the tenant name as key.
	if params.Shard == "" {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while creating routing plan for collection %q", r.collection)
	}
	_, writeReplicas, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while getting write replicas for collection %s shard %s: %w", r.collection, params.Shard, err)
	}

	if len(writeReplicas.Replicas) == 0 {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while checking write replica availability for collection %q shard %q", r.collection, params.Shard)
	}

	// Order replicas with direct candidate first
	orderedReplicas := sort(writeReplicas.Replicas, params.DirectCandidateNode, r.nodeSelector.LocalName())

	plan := types.WriteRoutingPlan{
		Shard: params.Shard,
		ReplicaSet: types.WriteReplicaSet{
			Replicas:           orderedReplicas,
			AdditionalReplicas: writeReplicas.AdditionalReplicas,
		},
		ConsistencyLevel: params.ConsistencyLevel,
	}

	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.WriteRoutingPlan{}, err
	}
	plan.IntConsistencyLevel = cl
	return plan, nil
}

// sort orders replicas with the direct candidate first, followed by the remaining replicas
func sort(replicas []types.Replica, directCandidate string, localNodeName string) []types.Replica {
	if len(replicas) == 0 {
		return replicas
	}

	preferredNodeName := directCandidate
	if preferredNodeName == "" {
		preferredNodeName = localNodeName
	}

	var orderedReplicas []types.Replica
	var otherReplicas []types.Replica

	for _, replica := range replicas {
		if replica.NodeName == preferredNodeName {
			orderedReplicas = append(orderedReplicas, replica)
		} else {
			otherReplicas = append(otherReplicas, replica)
		}
	}

	orderedReplicas = append(orderedReplicas, otherReplicas...)
	return orderedReplicas
}
