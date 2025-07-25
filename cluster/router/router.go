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

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/usecases/objects"

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

// sort orders replicas with the preferred node first, followed by the remaining replicas
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

func preferredNode(directCandidate string, localNodeName string) string {
	if directCandidate != "" {
		return directCandidate
	}
	return localNodeName
}

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
func (r *singleTenantRouter) GetReadWriteReplicasLocation(collection string, tenant string, shard string) (readReplicas types.ReadReplicaSet, writeReplicas types.WriteReplicaSet, err error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
	}
	return r.getReadWriteReplicasLocation(collection, tenant, shard)
}

// GetWriteReplicasLocation returns write replicas for single-tenant collections.
func (r *singleTenantRouter) GetWriteReplicasLocation(collection string, tenant string, shard string) (types.WriteReplicaSet, error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.WriteReplicaSet{}, err
	}
	_, writeReplicas, err := r.getReadWriteReplicasLocation(collection, tenant, shard)
	if err != nil {
		return types.WriteReplicaSet{}, err
	}
	return writeReplicas, nil
}

// GetReadReplicasLocation returns read replicas for single-tenant collections.
func (r *singleTenantRouter) GetReadReplicasLocation(collection string, tenant string, shard string) (types.ReadReplicaSet, error) {
	if err := r.validateTenant(tenant); err != nil {
		return types.ReadReplicaSet{}, err
	}
	readReplicas, _, err := r.getReadWriteReplicasLocation(collection, tenant, shard)
	if err != nil {
		return types.ReadReplicaSet{}, err
	}
	return readReplicas, nil
}

func (r *singleTenantRouter) getReadWriteReplicasLocation(collection string, tenant string, shard string) (readReplicas types.ReadReplicaSet, writeReplicas types.WriteReplicaSet, err error,
) {
	targetShards, err := r.targetShards(collection, shard)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
	}

	var allReadReplicas []types.Replica
	var allWriteReplicas []types.Replica
	var allAdditionalWriteReplicas []types.Replica

	for _, shardName := range targetShards {
		read, write, additional, err := r.replicasForShard(collection, tenant, shardName)
		if err != nil {
			return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
		}

		allReadReplicas = append(allReadReplicas, read...)
		allWriteReplicas = append(allWriteReplicas, write...)
		allAdditionalWriteReplicas = append(allAdditionalWriteReplicas, additional...)
	}

	return types.ReadReplicaSet{Replicas: allReadReplicas}, types.WriteReplicaSet{Replicas: allWriteReplicas, AdditionalReplicas: allAdditionalWriteReplicas}, nil
}

// targetShards returns either all shards or a single one, depending on the value of the shard parameter.
func (r *singleTenantRouter) targetShards(collection, shard string) ([]string, error) {
	if r.schemaReader == nil {
		return []string{}, fmt.Errorf("schema reader is nil")
	}
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

// replicasForShard gathers read/write/additional replicas for one shard.
func (r *singleTenantRouter) replicasForShard(collection, tenant, shard string) (
	read, write, additional []types.Replica, err error,
) {
	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error while getting replicas for collection %q shard %q: %w", collection, shard, err)
	}

	readNodeNames := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	writeNodeNames, additionalWriteNodeNames := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)

	read = buildReplicas(readNodeNames, shard, r.nodeSelector.NodeHostname)
	write = buildReplicas(writeNodeNames, shard, r.nodeSelector.NodeHostname)
	additional = buildReplicas(additionalWriteNodeNames, shard, r.nodeSelector.NodeHostname)

	return read, write, additional, nil
}

// BuildReadRoutingPlan constructs a read routing plan for single-tenant collections.
func (r *singleTenantRouter) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	if err := r.validateTenant(params.Tenant); err != nil {
		return types.ReadRoutingPlan{}, err
	}
	return r.buildReadRoutingPlan(params)
}

func (r *singleTenantRouter) buildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	readReplicas, _, err := r.getReadWriteReplicasLocation(r.collection, params.Tenant, params.Shard)
	if err != nil {
		return types.ReadRoutingPlan{}, err
	}

	if len(readReplicas.Replicas) == 0 {
		return types.ReadRoutingPlan{}, fmt.Errorf("no read replica found")
	}

	cl, err := readReplicas.ValidateConsistencyLevel(params.ConsistencyLevel)
	if err != nil {
		return types.ReadRoutingPlan{}, err
	}

	orderedReplicas := sort(readReplicas.Replicas, preferredNode(params.DirectCandidateNode, r.nodeSelector.LocalName()))

	plan := types.ReadRoutingPlan{
		Shard:  params.Shard,
		Tenant: params.Tenant,
		ReplicaSet: types.ReadReplicaSet{
			Replicas: orderedReplicas,
		},
		ConsistencyLevel:    params.ConsistencyLevel,
		IntConsistencyLevel: cl,
	}

	return plan, nil
}

// BuildWriteRoutingPlan constructs a write routing plan for single-tenant collections.
func (r *singleTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	if err := r.validateTenant(params.Tenant); err != nil {
		return types.WriteRoutingPlan{}, err
	}
	return r.buildWriteRoutingPlan(params)
}

func (r *singleTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	_, writeReplicas, err := r.getReadWriteReplicasLocation(r.collection, params.Tenant, params.Shard)
	if err != nil {
		return types.WriteRoutingPlan{}, err
	}

	if len(writeReplicas.Replicas) == 0 {
		return types.WriteRoutingPlan{}, fmt.Errorf("no write replica found")
	}

	cl, err := writeReplicas.ValidateConsistencyLevel(params.ConsistencyLevel)
	if err != nil {
		return types.WriteRoutingPlan{}, err
	}

	sortedWriteReplicas := sort(writeReplicas.Replicas, preferredNode(params.DirectCandidateNode, r.nodeSelector.LocalName()))

	plan := types.WriteRoutingPlan{
		Shard:  params.Shard,
		Tenant: params.Tenant,
		ReplicaSet: types.WriteReplicaSet{
			Replicas:           sortedWriteReplicas,
			AdditionalReplicas: writeReplicas.AdditionalReplicas,
		},
		ConsistencyLevel:    params.ConsistencyLevel,
		IntConsistencyLevel: cl,
	}

	return plan, nil
}

func (r *singleTenantRouter) BuildRoutingPlanOptions(_, shard string, cl types.ConsistencyLevel, directCandidate string) types.RoutingPlanBuildOptions {
	return types.RoutingPlanBuildOptions{
		Shard:               shard,
		Tenant:              "",
		ConsistencyLevel:    cl,
		DirectCandidateNode: directCandidate,
	}
}

// validateTenant for a multi-tenant router checks the tenant is not empty and returns an error if it is.
func (r *multiTenantRouter) validateTenant(tenant string) error {
	if tenant == "" {
		return objects.NewErrMultiTenancy(fmt.Errorf("class %s has multi-tenancy enabled, but request was without tenant", r.collection))
	}
	return nil
}

// NodeHostname returns the hostname for the given node name in multi-tenant collections.
func (r *multiTenantRouter) NodeHostname(nodeName string) (string, bool) {
	return r.nodeSelector.NodeHostname(nodeName)
}

// AllHostnames returns all known hostnames in the cluster for multi-tenant collections.
func (r *multiTenantRouter) AllHostnames() []string {
	return r.nodeSelector.AllHostnames()
}

// GetReadWriteReplicasLocation returns read and write replicas for multi-tenant collections.
func (r *multiTenantRouter) GetReadWriteReplicasLocation(collection string, tenant string, shard string) (readReplicas types.ReadReplicaSet, writeReplicas types.WriteReplicaSet, err error) {
	if shard == "" {
		shard = tenant
	}
	if err := r.validateTenant(tenant); err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
	}
	if err := r.validateTenantShard(tenant, shard); err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
	}
	return r.getReadWriteReplicasLocation(collection, tenant, shard)
}

// GetWriteReplicasLocation returns write replicas for multi-tenant collections.
func (r *multiTenantRouter) GetWriteReplicasLocation(collection string, tenant string, shard string) (types.WriteReplicaSet, error) {
	if shard == "" {
		shard = tenant
	}
	if err := r.validateTenant(tenant); err != nil {
		return types.WriteReplicaSet{}, err
	}
	if err := r.validateTenantShard(tenant, shard); err != nil {
		return types.WriteReplicaSet{}, err
	}
	_, writeReplicas, err := r.getReadWriteReplicasLocation(collection, tenant, shard)
	return writeReplicas, err
}

// GetReadReplicasLocation returns read replicas for multi-tenant collections.
func (r *multiTenantRouter) GetReadReplicasLocation(collection string, tenant string, shard string) (types.ReadReplicaSet, error) {
	if shard == "" {
		shard = tenant
	}
	if err := r.validateTenant(tenant); err != nil {
		return types.ReadReplicaSet{}, err
	}
	if err := r.validateTenantShard(tenant, shard); err != nil {
		return types.ReadReplicaSet{}, err
	}
	readReplicas, _, err := r.getReadWriteReplicasLocation(collection, tenant, shard)
	return readReplicas, err
}

func (r *multiTenantRouter) getReadWriteReplicasLocation(collection string, tenant, shard string) (types.ReadReplicaSet, types.WriteReplicaSet, error) {
	tenantStatus, err := r.schemaGetter.OptimisticTenantStatus(context.TODO(), collection, tenant)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, objects.NewErrMultiTenancy(err)
	}

	if err = r.tenantExistsAndIsActive(tenantStatus, tenant); err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
	}

	replicas, err := r.schemaReader.ShardReplicas(collection, shard)
	if err != nil {
		return types.ReadReplicaSet{}, types.WriteReplicaSet{}, err
	}

	readNodeNames := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	readReplicas := buildReplicas(readNodeNames, shard, r.nodeSelector.NodeHostname)

	writeNodeNames, additionalWriteNodeNames := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)
	writeReplicas := buildReplicas(writeNodeNames, shard, r.nodeSelector.NodeHostname)
	additionalWriteReplicas := buildReplicas(additionalWriteNodeNames, shard, r.nodeSelector.NodeHostname)

	return types.ReadReplicaSet{Replicas: readReplicas}, types.WriteReplicaSet{Replicas: writeReplicas, AdditionalReplicas: additionalWriteReplicas}, nil
}

func (r *multiTenantRouter) tenantExistsAndIsActive(tenantStatus map[string]string, tenant string) error {
	status, ok := tenantStatus[tenant]
	if !ok {
		return objects.NewErrMultiTenancy(fmt.Errorf("%w: %q", enterrors.ErrTenantNotFound, tenant))
	}
	if status != models.TenantActivityStatusHOT {
		return objects.NewErrMultiTenancy(fmt.Errorf("%w: '%s'", enterrors.ErrTenantNotActive, tenant))
	}
	return nil
}

// BuildWriteRoutingPlan constructs a write routing plan for multi-tenant collections.
func (r *multiTenantRouter) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	if params.Shard == "" {
		params.Shard = params.Tenant
	}
	if err := r.validateTenant(params.Tenant); err != nil {
		return types.WriteRoutingPlan{}, err
	}
	return r.buildWriteRoutingPlan(params)
}

func (r *multiTenantRouter) buildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.WriteRoutingPlan, error) {
	_, writeReplicas, err := r.getReadWriteReplicasLocation(r.collection, params.Tenant, params.Shard)
	if err != nil {
		return types.WriteRoutingPlan{}, err
	}

	if len(writeReplicas.Replicas) == 0 {
		return types.WriteRoutingPlan{}, fmt.Errorf("no read replica found")
	}

	cl, err := writeReplicas.ValidateConsistencyLevel(params.ConsistencyLevel)
	if err != nil {
		return types.WriteRoutingPlan{}, err
	}

	// Order replicas with direct candidate first
	orderedReplicas := sort(writeReplicas.Replicas, preferredNode(params.DirectCandidateNode, r.nodeSelector.LocalName()))

	plan := types.WriteRoutingPlan{
		Shard:  params.Shard,
		Tenant: params.Tenant,
		ReplicaSet: types.WriteReplicaSet{
			Replicas:           orderedReplicas,
			AdditionalReplicas: writeReplicas.AdditionalReplicas,
		},
		ConsistencyLevel:    params.ConsistencyLevel,
		IntConsistencyLevel: cl,
	}

	return plan, nil
}

// BuildReadRoutingPlan constructs a read routing plan for multi-tenant collections.
func (r *multiTenantRouter) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	if params.Shard == "" {
		params.Shard = params.Tenant
	}
	if err := r.validateTenant(params.Tenant); err != nil {
		return types.ReadRoutingPlan{}, err
	}
	return r.buildReadRoutingPlan(params)
}

func (r *multiTenantRouter) buildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	readReplicas, _, err := r.getReadWriteReplicasLocation(r.collection, params.Tenant, params.Shard)
	if err != nil {
		return types.ReadRoutingPlan{}, err
	}

	if len(readReplicas.Replicas) == 0 {
		return types.ReadRoutingPlan{}, fmt.Errorf("no read replica found")
	}

	cl, err := readReplicas.ValidateConsistencyLevel(params.ConsistencyLevel)
	if err != nil {
		return types.ReadRoutingPlan{}, err
	}

	// Order replicas with direct candidate first
	orderedReplicas := sort(readReplicas.Replicas, preferredNode(params.DirectCandidateNode, r.nodeSelector.LocalName()))

	return types.ReadRoutingPlan{
		Shard:  params.Shard,
		Tenant: params.Tenant,
		ReplicaSet: types.ReadReplicaSet{
			Replicas: orderedReplicas,
		},
		ConsistencyLevel:    params.ConsistencyLevel,
		IntConsistencyLevel: cl,
	}, nil
}

func (r *multiTenantRouter) validateTenantShard(tenant, shard string) error {
	if shard != "" && tenant != "" && shard != tenant {
		return fmt.Errorf("invalid tenant shard %q, expected %q", shard, tenant)
	}

	return nil
}

func (r *multiTenantRouter) BuildRoutingPlanOptions(tenant, shard string, cl types.ConsistencyLevel, directCandidate string) types.RoutingPlanBuildOptions {
	return types.RoutingPlanBuildOptions{
		Shard:               shard,
		Tenant:              tenant,
		ConsistencyLevel:    cl,
		DirectCandidateNode: directCandidate,
	}
}
