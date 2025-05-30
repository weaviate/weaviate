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

// Package router provides an abstraction for determining the optimal routing plans
// for reads and writes within a Weaviate cluster. It handles logic around sharding,
// replication, and consistency, helping determine which nodes (replicas) should be
// queried for a given operation.
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
	"golang.org/x/exp/slices"
)

// Router defines the contract for determining routing plans for reads and writes
// within a cluster. It abstracts the logic to identify read/write replicas,
// construct routing plans, and access cluster host information including hostnames
// and ip addresses.
type Router interface {
	// GetReadWriteReplicasLocation returns the read and write replicas for a given
	// collection and tenant.
	//
	// Parameters:
	//   - collection: the name of the collection to get replicas for.
	//   - tenant: the tenant identifier (empty for single-tenant collections).
	//
	// Returns:
	//   - readReplicas: a list of node names serving as read replicas.
	//   - writeReplicas: a list of node names serving as write replicas.
	//   - error: if an error occurs while retrieving replicas or if the tenant is not found/active.
	GetReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, error)

	// GetWriteReplicasLocation returns the write replicas for a given collection and tenant.
	//
	// Parameters:
	//   - collection: the name of the collection to get write replicas for.
	//   - tenant: the tenant identifier (empty for single-tenant collections).
	//
	// Returns:
	//   - writeReplicas: a list of node names serving as write replicas.
	//   - error: if an error occurs while retrieving replicas or if the tenant is not found/active.
	GetWriteReplicasLocation(collection string, tenant string) ([]string, error)

	// GetReadReplicasLocation returns the read replicas for a given collection and tenant.
	//
	// Parameters:
	//   - collection: the name of the collection to get read replicas for.
	//   - tenant: the tenant identifier (empty for single-tenant collections).
	//
	// Returns:
	//   - readReplicas: a list of node names serving as read replicas.
	//   - error: if an error occurs while retrieving replicas or if the tenant is not found/active.
	GetReadReplicasLocation(collection string, tenant string) ([]string, error)

	// BuildReadRoutingPlan constructs a routing plan for reading data from the cluster including only shards
	// which the client is allowed to read.
	//
	// Parameters:
	//   - params: routing plan build options containing collection name, tenant, consistency level,
	//     and optional direct candidate replica preference.
	//
	// Returns:
	//   - RoutingPlan: the constructed read routing plan with ordered replicas and host addresses.
	//   - error: if validation fails, no suitable replicas are found, or consistency level is invalid.
	BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error)

	// BuildWriteRoutingPlan constructs a routing plan for writing data to the cluster including only shards
	// which the client is allowed to write.
	//
	// Parameters:
	//   - params: routing plan build options containing collection name, tenant, consistency level,
	//     and optional direct candidate replica preference.
	//
	// Returns:
	//   - RoutingPlan: the constructed write routing plan with ordered replicas and host addresses.
	//   - error: if validation fails, no suitable replicas are found, or consistency level is invalid.
	BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error)

	// NodeHostname returns the hostname for a given node name.
	//
	// Parameters:
	//   - nodeName: the name of the node to get the hostname for.
	//
	// Returns:
	//   - hostname: the hostname of the node.
	//   - ok: true if the hostname was found, false otherwise.
	NodeHostname(nodeName string) (string, bool)

	// AllHostnames returns all known hostnames in the cluster.
	//
	// Returns:
	//   - hostnames: a slice containing all known hostnames in the cluster.
	AllHostnames() []string
}

// RouterBuilder provides a builder for creating router instances based on configuration.
// Use NewBuilder() with all required parameters, then call Build() to get the appropriate Router implementation,
// either a multi-tenant router or a single tenant router. The multi-tenant router will use the tenant name as the
// partitioning key to identify a specific tenant's partitioning.
type RouterBuilder struct {
	className            string
	partitioningEnabled  bool
	clusterStateReader   cluster.NodeSelector
	schemaGetter         schema.SchemaGetter
	metadataReader       schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
}

// NewBuilder creates a new RouterBuilder with the provided configuration.
// All parameters except metadataReader are required. metadataReader is only required for single-tenant routers.
//
// Parameters:
//   - className: the name of the collection/class that this router will handle.
//   - partitioningEnabled: true for multi-tenant mode, false for single-tenant mode.
//   - clusterStateReader: provides cluster node state information and hostnames.
//   - schemaGetter: provides collection schemas, sharding states, and tenant information.
//   - metadataReader: provides shard replica metadata (required only for single-tenant routers, can be nil for multi-tenant).
//   - replicationFSMReader: provides replica state information for replication consistency.
//
// Returns:
//   - *RouterBuilder: a new builder instance ready to build the appropriate router.
func NewBuilder(
	className string,
	partitioningEnabled bool,
	clusterStateReader cluster.NodeSelector,
	schemaGetter schema.SchemaGetter,
	metadataReader schemaTypes.SchemaReader,
	replicationFSMReader replicationTypes.ReplicationFSMReader,
) *RouterBuilder {
	return &RouterBuilder{
		className:            className,
		partitioningEnabled:  partitioningEnabled,
		clusterStateReader:   clusterStateReader,
		schemaGetter:         schemaGetter,
		metadataReader:       metadataReader,
		replicationFSMReader: replicationFSMReader,
	}
}

// Build builds and returns the appropriate router implementation based on the partitioning configuration.
// The method validates all required dependencies and returns either a *multiTenantRouter or *singleTenantRouter
// depending on the partitioning setting provided to NewBuilder().
//
// Returns:
//   - Router: a concrete router implementation (*multiTenantRouter or *singleTenantRouter) that implements the Router interface.
//   - error: if any required dependencies are missing or validation fails.
func (b *RouterBuilder) Build() (Router, error) {
	// Validate required dependencies
	if b.className == "" {
		return nil, fmt.Errorf("className is required")
	}
	if b.schemaGetter == nil {
		return nil, fmt.Errorf("schemaGetter is required")
	}
	if b.replicationFSMReader == nil {
		return nil, fmt.Errorf("replicationFSMReader is required")
	}
	if b.clusterStateReader == nil {
		return nil, fmt.Errorf("clusterStateReader is required")
	}

	if b.partitioningEnabled {
		return &multiTenantRouter{
			className:            b.className,
			schemaGetter:         b.schemaGetter,
			replicationFSMReader: b.replicationFSMReader,
			clusterStateReader:   b.clusterStateReader,
		}, nil
	}

	// Additional validation for single-tenant router
	if b.metadataReader == nil {
		return nil, fmt.Errorf("metadataReader is required for single-tenant router")
	}

	return &singleTenantRouter{
		className:            b.className,
		schemaGetter:         b.schemaGetter,
		metadataReader:       b.metadataReader,
		replicationFSMReader: b.replicationFSMReader,
		clusterStateReader:   b.clusterStateReader,
	}, nil
}

// multiTenantRouter is the implementation of Router for multi-tenant collections.
// In multi-tenant mode, tenant isolation is achieved through partitioning using
// the tenant name as the partitioning key. Each tenant effectively becomes its own shard.
// Exported to allow direct instantiation and type assertions when concrete type access is needed.
type multiTenantRouter struct {
	className            string
	schemaGetter         schema.SchemaGetter
	replicationFSMReader replicationTypes.ReplicationFSMReader
	clusterStateReader   cluster.NodeSelector
}

// singleTenantRouter is the implementation of Router for single-tenant collections.
// In single-tenant mode, data is distributed across multiple physical shards without
// tenant-based partitioning. All data belongs to a single logical tenant.
// Exported to allow direct instantiation and type assertions when concrete type access is needed.
type singleTenantRouter struct {
	className            string
	schemaGetter         schema.SchemaGetter
	metadataReader       schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	clusterStateReader   cluster.NodeSelector
}

// GetReadWriteReplicasLocation returns read and write replicas for single-tenant collections.
// In single-tenant mode, this method aggregates replicas from all physical shards of the collection.
//
// Parameters:
//   - collection: the name of the collection to get replicas for.
//   - tenant: ignored in single-tenant mode (can be empty string).
//
// Returns:
//   - readReplicas: a list of node names serving as read replicas across all shards.
//   - writeReplicas: a list of node names serving as write replicas across all shards.
//   - error: if an error occurs while retrieving shard information or replica states.
func (r *singleTenantRouter) GetReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, error) {
	err := r.validateTenant(tenant)
	if err != nil {
		return nil, nil, err
	}
	readReplicas, writeReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return nil, nil, err
	}
	return readReplicas, writeReplicas, nil
}

// GetWriteReplicasLocation returns write replicas for single-tenant collections.
// In single-tenant mode, this method aggregates write replicas from all physical shards of the collection.
//
// Parameters:
//   - collection: the name of the collection to get write replicas for.
//   - tenant: ignored in single-tenant mode (can be empty string).
//
// Returns:
//   - writeReplicas: a list of node names serving as write replicas across all shards.
//   - error: if an error occurs while retrieving shard information or replica states.
func (r *singleTenantRouter) GetWriteReplicasLocation(collection string, tenant string) ([]string, error) {
	err := r.validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	_, writeReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return nil, err
	}
	return writeReplicas, nil
}

// GetReadReplicasLocation returns read replicas for single-tenant collections.
// In single-tenant mode, this method aggregates read replicas from all physical shards of the collection.
//
// Parameters:
//   - collection: the name of the collection to get read replicas for.
//   - tenant: ignored in single-tenant mode (can be empty string).
//
// Returns:
//   - readReplicas: a list of node names serving as read replicas across all shards.
//   - error: if an error occurs while retrieving shard information or replica states.
func (r *singleTenantRouter) GetReadReplicasLocation(collection string, tenant string) ([]string, error) {
	err := r.validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	readReplicas, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return nil, err
	}
	return readReplicas, nil
}

func (r *singleTenantRouter) getReadWriteReplicasLocation(collection string, _ string) ([]string, []string, error) {
	shards := r.schemaGetter.CopyShardingState(collection).AllPhysicalShards()
	readReplicaSet := make(map[string]bool)
	writeReplicaSet := make(map[string]bool)

	for _, shard := range shards {
		replicas, err := r.metadataReader.ShardReplicas(collection, shard)
		if err != nil {
			return nil, nil, fmt.Errorf("could not get replicas for shard %q: %w", shard, err)
		}
		reads, writes := r.replicationFSMReader.FilterOneShardReplicasReadWrite(collection, shard, replicas)

		for _, read := range reads {
			readReplicaSet[read] = true
		}
		for _, write := range writes {
			writeReplicaSet[write] = true
		}
	}

	readReplicas := make([]string, 0, len(readReplicaSet))
	for replica := range readReplicaSet {
		readReplicas = append(readReplicas, replica)
	}

	writeReplicas := make([]string, 0, len(writeReplicaSet))
	for replica := range writeReplicaSet {
		writeReplicas = append(writeReplicas, replica)
	}

	return readReplicas, writeReplicas, nil
}

// BuildReadRoutingPlan constructs a read routing plan for single-tenant collections.
// The routing plan includes ordered replicas with preference for direct candidates and host addresses.
//
// Parameters:
//   - params: routing plan build options containing collection name, tenant (ignored),
//     consistency level, and optional direct candidate replica preference.
//
// Returns:
//   - RoutingPlan: the constructed read routing plan with replicas ordered by preference.
//   - error: if no replicas are found or consistency level validation fails.
func (r *singleTenantRouter) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	return r.buildReadRoutingPlan(params)
}

// BuildWriteRoutingPlan constructs a write routing plan for single-tenant collections.
// The routing plan includes ordered replicas with preference for direct candidates and host addresses.
//
// Parameters:
//   - params: routing plan build options containing collection name, tenant (ignored),
//     consistency level, and optional direct candidate replica preference.
//
// Returns:
//   - RoutingPlan: the constructed write routing plan with replicas ordered by preference.
//   - error: if no replicas are found or consistency level validation fails.
func (r *singleTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	return r.buildReadRoutingPlan(params)
}

// NodeHostname returns the hostname for the given node name in single-tenant collections.
//
// Parameters:
//   - nodeName: the name of the node to get the hostname for.
//
// Returns:
//   - hostname: the hostname of the node.
//   - ok: true if the hostname was found, false otherwise.
func (r *singleTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.clusterStateReader.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for single-tenant collections.
//
// Returns:
//   - hostnames: a slice containing all known hostnames in the cluster.
func (r *singleTenantRouter) AllHostnames() []string {
	return r.clusterStateReader.AllHostnames()
}

func (r *singleTenantRouter) buildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	shards := r.schemaGetter.CopyShardingState(r.className).AllPhysicalShards()
	replicaSet := make(map[string]bool)

	for _, shard := range shards {
		replicas, err := r.metadataReader.ShardReplicas(params.Collection, shard)
		if err != nil {
			return types.RoutingPlan{}, fmt.Errorf("could not get replicas for shard %q: %w", shard, err)
		}

		readReplicas, _ := r.replicationFSMReader.FilterOneShardReplicasReadWrite(params.Collection, shard, replicas)
		for _, readReplica := range readReplicas {
			replicaSet[readReplica] = true
		}
	}

	replicas := make([]string, 0, len(replicaSet))
	for replica := range replicaSet {
		replicas = append(replicas, replica)
	}

	return buildRoutingPlan(
		params.Collection,
		params.Shard,
		params.DirectCandidateReplica,
		replicas,
		params.ConsistencyLevel,
		r.clusterStateReader,
	)
}

func (r *singleTenantRouter) validateTenant(tenant string) error {
	if tenant != "" {
		return fmt.Errorf("tenant must be empty for single-tenant collections, got: %q", tenant)
	}
	return nil
}

// GetReadWriteReplicasLocation returns read and write replicas for multi-tenant collections.
// In multi-tenant mode, each tenant acts as its own shard, so replicas are determined
// based on the specific tenant's status and replication configuration.
//
// Parameters:
//   - collection: the name of the multi-tenant collection.
//   - tenant: the tenant identifier whose replicas should be retrieved.
//
// Returns:
//   - readReplicas: a list of node names serving as read replicas for the tenant.
//   - writeReplicas: a list of node names serving as write replicas for the tenant.
//   - error: if the tenant is not found, not active (HOT status), or other errors occur.
func (r *multiTenantRouter) GetReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, error) {
	err := r.validateTenant(tenant)
	if err != nil {
		return nil, nil, err
	}
	return r.getReadWriteReplicasLocation(collection, tenant)
}

func (r *multiTenantRouter) getReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, error) {
	tenantShards, err := r.schemaGetter.OptimisticTenantStatus(context.TODO(), collection, tenant)
	if err != nil {
		return nil, nil, err
	}

	status, ok := tenantShards[tenant]
	if !ok {
		return nil, nil, fmt.Errorf("tenant not found: %q", tenant)
	}

	if status != models.TenantActivityStatusHOT {
		return nil, nil, fmt.Errorf("tenant not active: %q", tenant)
	}

	readReplicas, writeReplicas := r.replicationFSMReader.FilterOneShardReplicasReadWrite(collection, tenant, []string{tenant})
	return readReplicas, writeReplicas, nil
}

// GetWriteReplicasLocation returns write replicas for multi-tenant collections.
// In multi-tenant mode, this method gets write replicas for the specific tenant.
//
// Parameters:
//   - collection: the name of the multi-tenant collection.
//   - tenant: the tenant identifier whose write replicas should be retrieved.
//
// Returns:
//   - writeReplicas: a list of node names serving as write replicas for the tenant.
//   - error: if the tenant is not found, not active (HOT status), or other errors occur.
func (r *multiTenantRouter) GetWriteReplicasLocation(collection string, tenant string) ([]string, error) {
	err := r.validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	_, writeReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	return writeReplicas, err
}

// GetReadReplicasLocation returns read replicas for multi-tenant collections.
// In multi-tenant mode, this method gets read replicas for the specific tenant.
//
// Parameters:
//   - collection: the name of the multi-tenant collection.
//   - tenant: the tenant identifier whose read replicas should be retrieved.
//
// Returns:
//   - readReplicas: a list of node names serving as read replicas for the tenant.
//   - error: if the tenant is not found, not active (HOT status), or other errors occur.
func (r *multiTenantRouter) GetReadReplicasLocation(collection string, tenant string) ([]string, error) {
	err := r.validateTenant(tenant)
	if err != nil {
		return nil, err
	}
	readReplicas, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	return readReplicas, err
}

// BuildReadRoutingPlan constructs a read routing plan for multi-tenant collections.
// The routing plan includes ordered replicas specific to the tenant with preference for direct candidates.
//
// Parameters:
//   - params: routing plan build options containing collection name, tenant identifier,
//     consistency level, and optional direct candidate replica preference.
//
// Returns:
//   - RoutingPlan: the constructed read routing plan with tenant-specific replicas ordered by preference.
//   - error: if the tenant is not found/active, no replicas are available, or consistency level validation fails.
func (r *multiTenantRouter) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	return r.buildReadRoutingPlan(params)
}

// BuildWriteRoutingPlan constructs a write routing plan for multi-tenant collections.
// The routing plan includes ordered replicas specific to the tenant with preference for direct candidates.
//
// Parameters:
//   - params: routing plan build options containing collection name, tenant identifier,
//     consistency level, and optional direct candidate replica preference.
//
// Returns:
//   - RoutingPlan: the constructed write routing plan with tenant-specific replicas ordered by preference.
//   - error: if the tenant is not found/active, no replicas are available, or consistency level validation fails.
func (r *multiTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	return r.buildReadRoutingPlan(params)
}

// NodeHostname returns the hostname for the given node name in multi-tenant collections.
//
// Parameters:
//   - nodeName: the name of the node to get the hostname for.
//
// Returns:
//   - hostname: the hostname of the node.
//   - ok: true if the hostname was found, false otherwise.
func (r *multiTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.clusterStateReader.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for multi-tenant collections.
//
// Returns:
//   - hostnames: a slice containing all known hostnames in the cluster.
func (r *multiTenantRouter) AllHostnames() []string {
	return r.clusterStateReader.AllHostnames()
}

func (r *multiTenantRouter) buildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	readReplicas, _, err := r.GetReadWriteReplicasLocation(params.Collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get read replicas location from sharding state: %w", err)
	}

	return buildRoutingPlan(
		params.Collection,
		params.Shard,
		params.DirectCandidateReplica,
		readReplicas,
		params.ConsistencyLevel,
		r.clusterStateReader,
	)
}

func buildRoutingPlan(
	collection string,
	tenant string,
	directCandidate string,
	replicas []string,
	consistencyLevel types.ConsistencyLevel,
	clusterStateReader cluster.NodeSelector,
) (types.RoutingPlan, error) {
	plan := types.RoutingPlan{
		Collection:        collection,
		Shard:             tenant,
		Replicas:          make([]string, 0, len(replicas)),
		ConsistencyLevel:  consistencyLevel,
		ReplicasHostAddrs: make([]string, 0, len(replicas)),
	}

	if directCandidate == "" {
		directCandidate = clusterStateReader.LocalName()
	}

	for _, replica := range replicas {
		if replicaAddr, ok := clusterStateReader.NodeHostname(replica); ok {
			if replica == directCandidate {
				plan.Replicas = slices.Insert(plan.Replicas, 0, replica)
				plan.ReplicasHostAddrs = slices.Insert(plan.ReplicasHostAddrs, 0, replicaAddr)
			} else {
				plan.Replicas = append(plan.Replicas, replica)
				plan.ReplicasHostAddrs = append(plan.ReplicasHostAddrs, replicaAddr)
			}
		}
	}

	if len(plan.Replicas) == 0 {
		return plan, fmt.Errorf("no replicas found for class %s tenant %s", collection, tenant)
	}

	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.RoutingPlan{}, err
	}
	plan.IntConsistencyLevel = cl
	return plan, nil
}

func (r *multiTenantRouter) validateTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("tenant is required for multi-tenant collections")
	}
	return nil
}
