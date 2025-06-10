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
	//   - writeReplicas: a list of node names serving as primary write replicas.
	//   - additionalWriteReplicas: a list of node names serving as additional write replicas.
	//   - error: if an error occurs while retrieving replicas or if the tenant is not found/active.
	GetReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, []string, error)

	// GetWriteReplicasLocation returns the write replicas for a given collection and tenant.
	//
	// Parameters:
	//   - collection: the name of the collection to get write replicas for.
	//   - tenant: the tenant identifier (empty for single-tenant collections).
	//
	// Returns:
	//   - writeReplicas: a list of node names serving as primary write replicas.
	//   - additionalWriteReplicas: a list of node names serving as additional write replicas.
	//   - error: if an error occurs while retrieving replicas or if the tenant is not found/active (multi-tenant).
	GetWriteReplicasLocation(collection string, tenant string) ([]string, []string, error)

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
	//   - RoutingPlan: the routing plan includes replicas ordered with the direct candidate first,
	//     followed by the remaining replicas in no guaranteed order. If no direct candidate is provided,
	//     the local node name is used and placed first as the local replica.
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
	//   - ok: true if the hostname was found, false if the node name is unknown or unregistered.
	NodeHostname(nodeName string) (string, bool)

	// AllHostnames returns all known hostnames in the cluster.
	//
	// Returns:
	//   - hostnames: a slice of all known hostnames; always returns a valid slice, possibly empty.
	AllHostnames() []string
}

// RouterBuilder provides a builder for creating router instances based on configuration.
// Use NewBuilder() with all required parameters, then call Build() to get the appropriate Router implementation,
// either a multi-tenant router or a single tenant router. The multi-tenant router will use the tenant name as the
// partitioning key to identify a specific tenant's partitioning.
type RouterBuilder struct {
	collection           string
	partitioningEnabled  bool
	clusterStateReader   cluster.NodeSelector
	schemaGetter         schema.SchemaGetter
	metadataReader       schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
}

// NewBuilder creates a new RouterBuilder with the provided configuration.
//
// Parameters:
//   - collection: the name of the collection that this router will handle.
//   - partitioningEnabled: true for multi-tenant mode, false for single-tenant mode.
//   - clusterStateReader: provides cluster node state information and hostnames.
//   - schemaGetter: provides collection schemas, sharding states, and tenant information.
//   - metadataReader: provides shard replica (or node names) metadata.
//   - replicationFSMReader: provides replica state information for replication consistency.
//
// Returns:
//   - *RouterBuilder: a new builder instance ready to build the appropriate router.
func NewBuilder(
	collection string,
	partitioningEnabled bool,
	clusterStateReader cluster.NodeSelector,
	schemaGetter schema.SchemaGetter,
	metadataReader schemaTypes.SchemaReader,
	replicationFSMReader replicationTypes.ReplicationFSMReader,
) *RouterBuilder {
	return &RouterBuilder{
		collection:           collection,
		partitioningEnabled:  partitioningEnabled,
		clusterStateReader:   clusterStateReader,
		schemaGetter:         schemaGetter,
		metadataReader:       metadataReader,
		replicationFSMReader: replicationFSMReader,
	}
}

// Build builds and returns the appropriate router implementation based on the partitioning configuration.
//
// Returns:
//   - Router: a concrete router implementation (*multiTenantRouter or *singleTenantRouter) that implements the Router interface.
func (b *RouterBuilder) Build() Router {
	if b.partitioningEnabled {
		return &multiTenantRouter{
			collection:           b.collection,
			schemaGetter:         b.schemaGetter,
			metadataReader:       b.metadataReader,
			replicationFSMReader: b.replicationFSMReader,
			clusterStateReader:   b.clusterStateReader,
		}
	}
	return &singleTenantRouter{
		collection:           b.collection,
		schemaGetter:         b.schemaGetter,
		metadataReader:       b.metadataReader,
		replicationFSMReader: b.replicationFSMReader,
		clusterStateReader:   b.clusterStateReader,
	}
}

// multiTenantRouter is the implementation of Router for multi-tenant collections.
// In multi-tenant mode, tenant isolation is achieved through partitioning using
// the tenant name as the partitioning key. Each tenant effectively becomes its own shard.
type multiTenantRouter struct {
	collection           string
	schemaGetter         schema.SchemaGetter
	metadataReader       schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	clusterStateReader   cluster.NodeSelector
}

// singleTenantRouter is the implementation of Router for single-tenant collections.
// In single-tenant mode, data is distributed across multiple physical shards without
// tenant-based partitioning. All data belongs to a single logical tenant.
type singleTenantRouter struct {
	collection           string
	schemaGetter         schema.SchemaGetter
	metadataReader       schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	clusterStateReader   cluster.NodeSelector
}

// Interface compliance check at compile time.
var (
	_ Router = (*multiTenantRouter)(nil)
	_ Router = (*singleTenantRouter)(nil)
)

// GetReadWriteReplicasLocation returns read and write replicas for single-tenant collections.
// In single-tenant mode, this method aggregates replicas from all physical shards of the collection.
//
// Parameters:
//   - collection: the name of the collection to get replicas for.
//   - tenant: ignored in single-tenant mode (can be empty string).
//
// Returns:
//   - readReplicas: a list of node names serving as read replicas across all shards.
//   - writeReplicas: a list of node names serving as primary write replicas across all shards.
//   - additionalWriteReplicas: a list of node names serving as additional write replicas across all shards.
//   - error: if an error occurs while retrieving shard information or replica states.
func (r *singleTenantRouter) GetReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, []string, error) {
	if err := r.validateTenant(tenant); err != nil {
		return nil, nil, nil, err
	}
	return r.getReadWriteReplicasLocation(collection, tenant)
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
//   - additionalWriteReplicas: a list of node names serving as additional write replicas (not yet part of sharding state).
//   - error: if an error occurs while retrieving shard information or replica states.
func (r *singleTenantRouter) GetWriteReplicasLocation(collection string, tenant string) ([]string, []string, error) {
	if err := r.validateTenant(tenant); err != nil {
		return nil, nil, err
	}
	_, writeReplicas, additionalWriteReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return nil, nil, err
	}
	return writeReplicas, additionalWriteReplicas, nil
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
	if err := r.validateTenant(tenant); err != nil {
		return nil, err
	}
	readReplicas, _, _, err := r.getReadWriteReplicasLocation(collection, tenant)
	if err != nil {
		return nil, err
	}
	return readReplicas, nil
}

func (r *singleTenantRouter) getReadWriteReplicasLocation(collection string, _ string) ([]string, []string, []string, error) {
	shards := r.schemaGetter.CopyShardingState(collection).AllPhysicalShards()
	allReadReplicas := make([]string, 0)
	allWriteReplicas := make([]string, 0)
	allAdditionalWriteReplicas := make([]string, 0)

	for _, shard := range shards {
		replicas, err := r.metadataReader.ShardReplicas(collection, shard)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("could not get replicas for shard %q: %w", shard, err)
		}

		readReplicas := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
		writeReplicas, additionalWriteReplicas := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)

		allReadReplicas = append(allReadReplicas, readReplicas...)
		allWriteReplicas = append(allWriteReplicas, writeReplicas...)
		allAdditionalWriteReplicas = append(allAdditionalWriteReplicas, additionalWriteReplicas...)
	}

	return deduplicate(allReadReplicas), deduplicate(allWriteReplicas), deduplicate(allAdditionalWriteReplicas), nil
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
	if err := params.Validate(); err != nil {
		return types.RoutingPlan{}, err
	}
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
	if err := params.Validate(); err != nil {
		return types.RoutingPlan{}, err
	}
	return r.buildWriteRoutingPlan(params)
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
	replicas, _, _, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get read replicas: %w", err)
	}

	return buildRoutingPlan(
		r.collection,
		params.Shard,
		params.DirectCandidateReplica,
		replicas,
		params.ConsistencyLevel,
		r.clusterStateReader,
	)
}

func (r *singleTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	_, replicas, _, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get write replicas: %w", err)
	}

	return buildRoutingPlan(
		r.collection,
		params.Shard,
		params.DirectCandidateReplica,
		replicas,
		params.ConsistencyLevel,
		r.clusterStateReader,
	)
}

// validateTenant for a single-tenant router checks the tenant is empty and returns an error if it is not.
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
//   - additionalWriteReplicas: a list of node names serving as additional write replicas (not yet part of sharding state) for the tenant
//   - error: if the tenant is not found, not active (HOT status), or other errors occur.
func (r *multiTenantRouter) GetReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, []string, error) {
	if err := r.validateTenant(tenant); err != nil {
		return nil, nil, nil, err
	}
	return r.getReadWriteReplicasLocation(collection, tenant)
}

func (r *multiTenantRouter) getReadWriteReplicasLocation(collection string, tenant string) ([]string, []string, []string, error) {
	tenantStatus, err := r.schemaGetter.OptimisticTenantStatus(context.TODO(), collection, tenant)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not get optimistic tenant status for tenant %q: %w", tenant, err)
	}

	ok, err := r.tenantExistsAndIsActive(tenantStatus, tenant)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while checking tenant status for tenant %q: %w", tenant, err)
	}
	if !ok {
		return nil, nil, nil, fmt.Errorf("error while retrieving tenant %q: %w", tenant, err)
	}

	// NOTE: In multi-tenant collections, each tenant maps to exactly one shard where the tenant name
	// serves as the partitioning key and shard identifier. Since we've verified the tenant exists
	// and is active, we can directly query replicas using the tenant name as the shard. Also note that
	// in a multi-tenant collection each tenant has one and only one shard, potentially with multiple replicas
	// distributed across nodes in the cluster.
	replicas, err := r.metadataReader.ShardReplicas(collection, tenant)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not get replicas for shard %q: %w", tenant, err)
	}

	reads := r.replicationFSMReader.FilterOneShardReplicasRead(collection, tenant, replicas)
	writes, additionalWrites := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, tenant, replicas)

	return deduplicate(reads), deduplicate(writes), deduplicate(additionalWrites), nil
}

// TODO: ideally the router should not know about tenant status and should delegate loading it to a separate component
// which should be injected as a dependency. Consider adding an interface for checking tenant status (i.e. TenantStatusProvider).
func (r *multiTenantRouter) tenantExistsAndIsActive(tenantStatus map[string]string, tenant string) (bool, error) {
	status, ok := tenantStatus[tenant]
	if !ok {
		return false, fmt.Errorf("tenant not found: %q", tenant)
	}

	if status != models.TenantActivityStatusHOT {
		return false, fmt.Errorf("tenant not active: %q", tenant)
	}
	return true, nil
}

// deduplicate returns a new slice containing the unique elements of the input.
// The order of elements in the input slice is preserved.
func deduplicate(values []string) []string {
	if len(values) <= 1 {
		return values
	}

	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))

	for _, v := range values {
		if _, exists := seen[v]; !exists {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
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
//   - additionalWriteReplicas: a list of node names serving as additional write replicas (not yet part of sharding state).
//   - error: if the tenant is not found, not active (HOT status), or other errors occur.
func (r *multiTenantRouter) GetWriteReplicasLocation(collection string, tenant string) ([]string, []string, error) {
	if err := r.validateTenant(tenant); err != nil {
		return nil, nil, err
	}
	_, writeReplicas, additionalWriteReplicas, err := r.getReadWriteReplicasLocation(collection, tenant)
	return writeReplicas, additionalWriteReplicas, err
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
	if err := r.validateTenant(tenant); err != nil {
		return nil, err
	}
	readReplicas, _, _, err := r.getReadWriteReplicasLocation(collection, tenant)
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
	if err := params.Validate(); err != nil {
		return types.RoutingPlan{}, err
	}
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
	if err := params.Validate(); err != nil {
		return types.RoutingPlan{}, err
	}
	return r.buildWriteRoutingPlan(params)
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
	replicas, _, _, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get read replicas for shard %q: %w", params.Shard, err)
	}

	return buildRoutingPlan(
		r.collection,
		params.Shard,
		params.DirectCandidateReplica,
		replicas,
		params.ConsistencyLevel,
		r.clusterStateReader,
	)
}

func (r *multiTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	_, replicas, _, err := r.getReadWriteReplicasLocation(r.collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get write replicas for shard %q: %w", params.Shard, err)
	}

	return buildRoutingPlan(
		r.collection,
		params.Shard,
		params.DirectCandidateReplica,
		replicas,
		params.ConsistencyLevel,
		r.clusterStateReader,
	)
}

// validateTenant for a multi-tenant router checks the tenant is not empty and returns an error if it is.
func (r *multiTenantRouter) validateTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("tenant is required for multi-tenant collections")
	}
	return nil
}

// buildRoutingPlan constructs a routing plan using the given replicas and consistency level.
// Returns an error if the consistency level is invalid or no suitable replica is available.
func buildRoutingPlan(
	collection string,
	tenant string,
	directCandidate string,
	replicas []string,
	consistencyLevel types.ConsistencyLevel,
	clusterStateReader cluster.NodeSelector,
) (types.RoutingPlan, error) {
	plan := types.RoutingPlan{
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
		return plan, fmt.Errorf("no replicas found for collection %s tenant %s", collection, tenant)
	}
	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.RoutingPlan{}, err
	}
	plan.IntConsistencyLevel = cl
	return plan, err
}
