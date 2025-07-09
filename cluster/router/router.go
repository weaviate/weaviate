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

	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/usecases/schema"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Router defines the contract for determining routing plans for reads and writes
// within a cluster. It abstracts the logic to identify read/write replicas,
// construct routing plans, and access cluster host information including hostnames
// and ip addresses.
type Router interface {
	// GetReadWriteReplicasLocation returns the read and write replicas for a given
	// collection.
	//
	// Parameters:
	//   - collection: the name of the collection to get replicas for.
	//   - shard: the shard identifier (matches the tenant name for multi-tenant collections).
	//
	// Returns:
	//   - readReplicas: a replica set serving as read replicas.
	//   - writeReplicas: a replica set serving as primary write replicas.
	//   - additionalWriteReplicas: a replica set serving as additional write replicas.
	//   - error: if an error occurs while retrieving replicas.
	GetReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReplicaSet, writeReplicas types.ReplicaSet, additionalWriteReplicas types.ReplicaSet, err error)

	// GetWriteReplicasLocation returns the write replicas for a given collection.
	//
	// Parameters:
	//   - collection: the name of the collection to get write replicas for.
	//   - shard: the shard identifier (matches the tenant name for multi-tenant collections).
	//
	// Returns:
	//   - writeReplicas: a replica set serving as primary write replicas.
	//   - additionalWriteReplicas: a replica set serving as additional write replicas.
	//   - error: if an error occurs while retrieving replicas.
	GetWriteReplicasLocation(collection string, shard string) (types.ReplicaSet, types.ReplicaSet, error)

	// GetReadReplicasLocation returns the read replicas for a given collection.
	//
	// Parameters:
	//   - collection: the name of the collection to get read replicas for.
	//   - shard: the shard identifier (matches the tenant name for multi-tenant collections).
	//
	// Returns:
	//   - readReplicas: a replica set serving as read replicas.
	//   - error: if an error occurs while retrieving replicas.
	GetReadReplicasLocation(collection string, shard string) (types.ReplicaSet, error)

	// BuildReadRoutingPlan constructs a routing plan for reading data from the cluster including only shards
	// which the client is allowed to read.
	//
	// Parameters:
	//   - params: routing plan build options containing collection name, (optional) shard name, consistency level,
	//     and optional direct candidate replica preference.
	//
	// Returns:
	//   - routingPlan: the routing plan includes replicas ordered with the direct candidate first,
	//     followed by the remaining replicas in no guaranteed order. If no direct candidate is provided,
	//     the local node name is used and placed first as the local replica.
	//   - error: if validation fails, no suitable replicas are found, or consistency level is invalid.
	BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error)

	// BuildWriteRoutingPlan constructs a routing plan for writing data to the cluster including only shards
	// which the client is allowed to write.
	//
	// Parameters:
	//   - params: routing plan build options containing collection name, (optional) shard name), consistency level,
	//     and optional direct candidate replica preference.
	//
	// Returns:
	//   - routingPlan: the constructed write routing plan with ordered replicas.
	//   - error: if validation fails, no suitable replicas are found, or consistency level is invalid.
	BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error)

	// NodeHostname returns the hostname for a given node name.
	//
	// Parameters:
	//   - nodeName: the name of the node to get the hostname for.
	//
	// Returns:
	//   - hostname: the hostname of the node.
	//   - ok: true if the hostname was found, false if the node name is unknown or unregistered.
	NodeHostname(nodeName string) (string, bool)

	// AllHostnames returns all known hostnames in the cluster.
	//
	// Returns:
	//   - hostnames: a slice of all known hostnames; always returns a valid slice, possibly empty.
	AllHostnames() []string
}

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
func (b *Builder) Build() Router {
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

// Interface compliance check at compile time.
var (
	_ Router = (*multiTenantRouter)(nil)
	_ Router = (*singleTenantRouter)(nil)
)

// GetReadWriteReplicasLocation returns read and write replicas for single-tenant collections.
// In single-tenant mode, this method aggregates replicas from all physical shards of the collection.
func (r *singleTenantRouter) GetReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReplicaSet, writeReplicas types.ReplicaSet, additionalWriteReplicas types.ReplicaSet, err error) {
	return r.getReadWriteReplicasLocation(collection, shard)
}

// GetWriteReplicasLocation returns write replicas for single-tenant collections.
func (r *singleTenantRouter) GetWriteReplicasLocation(collection string, shard string) (types.ReplicaSet, types.ReplicaSet, error) {
	_, writeReplicas, additionalWriteReplicas, err := r.getReadWriteReplicasLocation(collection, shard)
	if err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, err
	}
	return writeReplicas, additionalWriteReplicas, nil
}

// GetReadReplicasLocation returns read replicas for single-tenant collections.
func (r *singleTenantRouter) GetReadReplicasLocation(collection string, tenant string) (types.ReplicaSet, error) {
	readReplicas, _, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return types.ReplicaSet{}, err
	}
	return readReplicas, nil
}

func (r *singleTenantRouter) getReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReplicaSet, writeReplicas types.ReplicaSet, additionalWriteReplicas types.ReplicaSet, err error,
) {
	targetShards, err := r.targetShards(collection, shard)
	if err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while getting target shards for collection %q shard %q: %w", collection, shard, err)
	}

	var allReadReplicas []types.Replica
	var allWriteReplicas []types.Replica
	var allAdditionalWriteReplicas []types.Replica

	for _, shardName := range targetShards {
		read, write, additional, err := r.replicasForShard(collection, shardName)
		if err != nil {
			return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shardName, err)
		}

		allReadReplicas = append(allReadReplicas, read...)
		allWriteReplicas = append(allWriteReplicas, write...)
		allAdditionalWriteReplicas = append(allAdditionalWriteReplicas, additional...)
	}

	return types.ReplicaSet{Replicas: allReadReplicas}, types.ReplicaSet{Replicas: allWriteReplicas}, types.ReplicaSet{Replicas: allAdditionalWriteReplicas}, nil
}

// targetShards returns either all shards or a single one, depending on input.
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
		return nil, fmt.Errorf("error while trying to find shard %s in collection %s", shard, collection)
	}
	return []string{shard}, nil
}

// replicasForShard gathers read/write/additional replicas for one shard.
func (r *singleTenantRouter) replicasForShard(collection, shard string) (
	read, write, additional []types.Replica, err error,
) {
	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shard, err)
	}

	readNodeNames := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	writeNodeNames, additionalWriteNodeNames := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)

	read = r.resolveNodeNamesToReplicas(readNodeNames, shard)
	write = r.resolveNodeNamesToReplicas(writeNodeNames, shard)
	additional = r.resolveNodeNamesToReplicas(additionalWriteNodeNames, shard)

	return read, write, additional, nil
}

// resolveNodeNamesToReplicas builds Replica objects from node names and shard name.
func (r *singleTenantRouter) resolveNodeNamesToReplicas(nodeNames []string, shard string) []types.Replica {
	var replicas []types.Replica
	for _, nodeName := range nodeNames {
		if hostAddr, ok := r.nodeSelector.NodeHostname(nodeName); ok {
			replicas = append(replicas, types.Replica{
				NodeName:  nodeName,
				ShardName: shard,
				HostAddr:  hostAddr,
			})
		}
	}
	return replicas
}

// BuildReadRoutingPlan constructs a read routing plan for single-tenant collections.
func (r *singleTenantRouter) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	return r.buildReadRoutingPlan(params)
}

// BuildWriteRoutingPlan constructs a write routing plan for single-tenant collections.
func (r *singleTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	return r.buildWriteRoutingPlan(params)
}

// NodeHostname returns the hostname for the given node name in single-tenant collections.
func (r *singleTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.nodeSelector.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for single-tenant collections.
func (r *singleTenantRouter) AllHostnames() []string {
	return r.nodeSelector.AllHostnames()
}

func (r *singleTenantRouter) buildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	readReplicas, _, _, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("error while getting read replicas for collection %q shard %q: %w", r.collection, params.Shard, err)
	}

	if len(readReplicas.Replicas) == 0 {
		return types.ReadRoutingPlan{}, fmt.Errorf("error while checking replica availability for collection %q shard %q", r.collection, params.Shard)
	}

	orderedReplicas := sort(readReplicas.Replicas, params.DirectCandidateNode, r.nodeSelector.LocalName())

	plan := types.ReadRoutingPlan{
		Shard: params.Shard,
		ReplicaSet: types.ReplicaSet{
			Replicas: orderedReplicas,
		},
		ConsistencyLevel: params.ConsistencyLevel,
	}

	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("error while validating consistency level: %w", err)
	}
	plan.IntConsistencyLevel = cl
	return plan, nil
}

func (r *singleTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	_, writeReplicas, additionalWriteReplicas, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
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
		ReplicaSet: types.ReplicaSet{
			Replicas: sortedWriteReplicas,
		},
		AdditionalReplicaSet: types.ReplicaSet{
			Replicas: additionalWriteReplicas.Replicas,
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

// GetReadWriteReplicasLocation returns read and write replicas for multi-tenant collections.
func (r *multiTenantRouter) GetReadWriteReplicasLocation(collection string, shard string) (readReplicas types.ReplicaSet, writeReplicas types.ReplicaSet, additionalWriteReplicas types.ReplicaSet, err error) {
	if err := r.validateTenant(shard); err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while validating tenant for collection %q shard %q: %w", collection, shard, err)
	}
	return r.getReadWriteReplicasLocation(collection, shard)
}

func (r *multiTenantRouter) getReadWriteReplicasLocation(collection string, shard string) (types.ReplicaSet, types.ReplicaSet, types.ReplicaSet, error) {
	tenantStatus, err := r.schemaGetter.OptimisticTenantStatus(context.TODO(), collection, shard)
	if err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while getting optimistic tenant status for tenant %q: %w", shard, err)
	}

	ok, err := r.tenantExistsAndIsActive(tenantStatus, shard)
	if err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while checking tenant status for tenant %q: %w", shard, err)
	}
	if !ok {
		return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while retrieving tenant %q: %w", shard, err)
	}

	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shard, err)
	}

	readNodeNames := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	readReplicas := buildReplicas(readNodeNames, shard, r.nodeSelector.NodeHostname)

	writeNodeNames, additionalWriteNodeNames := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)
	writeReplicas := buildReplicas(writeNodeNames, shard, r.nodeSelector.NodeHostname)
	additionalWriteReplicas := buildReplicas(additionalWriteNodeNames, shard, r.nodeSelector.NodeHostname)

	return types.ReplicaSet{Replicas: readReplicas}, types.ReplicaSet{Replicas: writeReplicas}, types.ReplicaSet{Replicas: additionalWriteReplicas}, nil
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
func (r *multiTenantRouter) GetWriteReplicasLocation(collection string, tenant string) (types.ReplicaSet, types.ReplicaSet, error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.ReplicaSet{}, types.ReplicaSet{}, fmt.Errorf("error while validating tenant for collection %q tenant %q: %w", collection, tenant, err)
	}
	_, writeReplicas, additionalWriteReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	return writeReplicas, additionalWriteReplicas, err
}

// GetReadReplicasLocation returns read replicas for multi-tenant collections.
func (r *multiTenantRouter) GetReadReplicasLocation(collection string, tenant string) (types.ReplicaSet, error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.ReplicaSet{}, fmt.Errorf("error while validating tenant for collection %q tenant %q: %w", collection, tenant, err)
	}
	readReplicas, _, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	return readReplicas, err
}

// BuildReadRoutingPlan constructs a read routing plan for multi-tenant collections.
func (r *multiTenantRouter) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	return r.buildReadRoutingPlan(params)
}

// BuildWriteRoutingPlan constructs a write routing plan for multi-tenant collections.
func (r *multiTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	return r.buildWriteRoutingPlan(params)
}

// NodeHostname returns the hostname for the given node name in multi-tenant collections.
func (r *multiTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.nodeSelector.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for multi-tenant collections.
func (r *multiTenantRouter) AllHostnames() []string {
	return r.nodeSelector.AllHostnames()
}

func (r *multiTenantRouter) buildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	readReplicas, _, _, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("error while building read routing plan for collection %q shard %q: %w", r.collection, params.Shard, err)
	}

	if len(readReplicas.Replicas) == 0 {
		return types.ReadRoutingPlan{}, fmt.Errorf("error while checking read replica availability for collection %q shard %q", r.collection, params.Shard)
	}

	// Order replicas with direct candidate first
	orderedReplicas := sort(readReplicas.Replicas, params.DirectCandidateNode, r.nodeSelector.LocalName())

	plan := types.ReadRoutingPlan{
		Shard: params.Shard,
		ReplicaSet: types.ReplicaSet{
			Replicas: orderedReplicas,
		},
		ConsistencyLevel: params.ConsistencyLevel,
	}

	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("error while validating consistency level: %w", err)
	}
	plan.IntConsistencyLevel = cl
	return plan, nil
}

func (r *multiTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	// Multi-tenant routing requires a specific tenant shard target to enforce isolation of tenants to specific shards.
	// Unlike single-tenant collections, broadcast writes (empty shard) are prohibited to prevent
	// cross-tenant data leakage. Each tenant maps to exactly one shard using the tenant name as key.
	if params.Shard == "" {
		return types.WriteRoutingPlan{}, fmt.Errorf("error while creating routing plan for collection %q", r.collection)
	}
	_, writeReplicas, additionalWriteReplicas, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
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
		ReplicaSet: types.ReplicaSet{
			Replicas: orderedReplicas,
		},
		AdditionalReplicaSet: types.ReplicaSet{
			Replicas: additionalWriteReplicas.Replicas,
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

// validateTenant for a multi-tenant router checks the tenant is not empty and returns an error if it is.
func (r *multiTenantRouter) validateTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("tenant is required for multi-tenant collections")
	}
	return nil
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
