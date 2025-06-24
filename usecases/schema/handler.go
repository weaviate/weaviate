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
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	ErrNotFound           = errors.New("not found")
	ErrUnexpectedMultiple = errors.New("unexpected multiple results")
)

// SchemaManager is responsible for consistent schema operations.
// It allows reading and writing the schema while directly talking to the leader, no matter which node it is.
// It also allows cluster related operations that can only be done on the leader (join/remove/stats/etc...)
// For details about each endpoint see [github.com/weaviate/weaviate/cluster.Raft].
// For local schema lookup where eventual consistency is acceptable, see [SchemaReader].
type SchemaManager interface {
	// Schema writes operation.
	AddClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error)
	RestoreClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error)
	UpdateClass(ctx context.Context, cls *models.Class, ss *sharding.State) (uint64, error)
	DeleteClass(ctx context.Context, name string) (uint64, error)
	AddProperty(ctx context.Context, class string, p ...*models.Property) (uint64, error)
	UpdateShardStatus(ctx context.Context, class, shard, status string) (uint64, error)
	AddTenants(ctx context.Context, class string, req *command.AddTenantsRequest) (uint64, error)
	UpdateTenants(ctx context.Context, class string, req *command.UpdateTenantsRequest) (uint64, error)
	DeleteTenants(ctx context.Context, class string, req *command.DeleteTenantsRequest) (uint64, error)

	// Cluster related operations
	Join(_ context.Context, nodeID, raftAddr string, voter bool) error
	Remove(_ context.Context, nodeID string) error
	Stats() map[string]any
	StorageCandidates() []string
	StoreSchemaV1() error

	// Strongly consistent schema read. These endpoints will emit a query to the leader to ensure that the data is read
	// from an up to date schema.
	QueryReadOnlyClasses(names ...string) (map[string]versioned.Class, error)
	QuerySchema() (models.Schema, error)
	QueryTenants(class string, tenants []string) ([]*models.Tenant, uint64, error)
	QueryCollectionsCount() (int, error)
	QueryShardOwner(class, shard string) (string, uint64, error)
	QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error)
	QueryShardingState(class string) (*sharding.State, uint64, error)
	QueryClassVersions(names ...string) (map[string]uint64, error)
}

// SchemaReader allows reading the local schema with or without using a schema version.
type SchemaReader interface {
	// WaitForUpdate ensures that the local schema has caught up to version.
	WaitForUpdate(ctx context.Context, version uint64) error

	// These schema reads function reads the metadata immediately present in the local schema and can be eventually
	// consistent.
	// For details about each endpoint see [github.com/weaviate/weaviate/cluster/schema.SchemaReader].
	ClassEqual(name string) string
	MultiTenancy(class string) models.MultiTenancyConfig
	ClassInfo(class string) (ci clusterSchema.ClassInfo)
	ReadOnlyClass(name string) *models.Class
	ReadOnlyVersionedClass(name string) versioned.Class
	ReadOnlySchema() models.Schema
	CopyShardingState(class string) *sharding.State
	ShardReplicas(class, shard string) ([]string, error)
	ShardFromUUID(class string, uuid []byte) string
	ShardOwner(class, shard string) (string, error)
	Read(class string, reader func(*models.Class, *sharding.State) error) error
	GetShardsStatus(class, tenant string) (models.ShardStatusList, error)

	// These schema reads function (...WithVersion) return the metadata once the local schema has caught up to the
	// version parameter. If version is 0 is behaves exactly the same as eventual consistent reads.
	// For details about each endpoint see [github.com/weaviate/weaviate/cluster/schema.VersionedSchemaReader].
	ClassInfoWithVersion(ctx context.Context, class string, version uint64) (clusterSchema.ClassInfo, error)
	MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error)
	ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error)
	ShardOwnerWithVersion(ctx context.Context, lass, shard string, version uint64) (string, error)
	ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (string, error)
	ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) ([]string, error)
	TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (map[string]string, error)
	CopyShardingStateWithVersion(ctx context.Context, class string, version uint64) (*sharding.State, error)
}

type validator interface {
	ValidateVectorIndexConfigUpdate(old, updated schemaConfig.VectorIndexConfig) error
	ValidateInvertedIndexConfigUpdate(old, updated *models.InvertedIndexConfig) error
	ValidateVectorIndexConfigsUpdate(old, updated map[string]schemaConfig.VectorIndexConfig) error
}

// The handler manages API requests for manipulating class schemas.
// This separation of responsibilities helps decouple these tasks
// from the Manager class, which combines many unrelated functions.
// By delegating these clear responsibilities to the handler, it maintains
// a clean separation from the manager, enhancing code modularity and maintainability.
type Handler struct {
	schemaManager SchemaManager
	schemaReader  SchemaReader

	cloud modulecapabilities.OffloadCloud

	validator validator

	logger                  logrus.FieldLogger
	Authorizer              authorization.Authorizer
	schemaConfig            *config.SchemaHandlerConfig
	config                  config.Config
	vectorizerValidator     VectorizerValidator
	moduleConfig            ModuleConfig
	clusterState            clusterState
	configParser            VectorConfigParser
	invertedConfigValidator InvertedConfigValidator
	scaleOut                scaleOut
	parser                  Parser
	classGetter             *ClassGetter

	asyncIndexingEnabled bool
}

// NewHandler creates a new handler
func NewHandler(
	schemaReader SchemaReader,
	schemaManager SchemaManager,
	validator validator,
	logger logrus.FieldLogger, authorizer authorization.Authorizer, schemaConfig *config.SchemaHandlerConfig,
	config config.Config,
	configParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	scaleoutManager scaleOut,
	cloud modulecapabilities.OffloadCloud,
	parser Parser, classGetter *ClassGetter,
) (Handler, error) {
	handler := Handler{
		config:                  config,
		schemaConfig:            schemaConfig,
		schemaReader:            schemaReader,
		schemaManager:           schemaManager,
		parser:                  parser,
		validator:               validator,
		logger:                  logger,
		Authorizer:              authorizer,
		configParser:            configParser,
		vectorizerValidator:     vectorizerValidator,
		invertedConfigValidator: invertedConfigValidator,
		moduleConfig:            moduleConfig,
		clusterState:            clusterState,
		scaleOut:                scaleoutManager,
		cloud:                   cloud,
		classGetter:             classGetter,

		asyncIndexingEnabled: entcfg.Enabled(os.Getenv("ASYNC_INDEXING")),
	}

	handler.scaleOut.SetSchemaReader(schemaReader)

	return handler, nil
}

// GetSchema retrieves a locally cached copy of the schema
func (h *Handler) GetConsistentSchema(ctx context.Context, principal *models.Principal, consistency bool) (schema.Schema, error) {
	var fullSchema schema.Schema
	if !consistency {
		fullSchema = h.getSchema()
	} else {
		consistentSchema, err := h.schemaManager.QuerySchema()
		if err != nil {
			return schema.Schema{}, fmt.Errorf("could not read schema with strong consistency: %w", err)
		}
		fullSchema = schema.Schema{
			Objects: &consistentSchema,
		}
	}

	filteredClasses := filter.New[*models.Class](h.Authorizer, h.config.Authorization.Rbac).Filter(
		ctx,
		h.logger,
		principal,
		fullSchema.Objects.Classes,
		authorization.READ,
		func(class *models.Class) string {
			return authorization.CollectionsMetadata(class.Class)[0]
		},
	)

	return schema.Schema{
		Objects: &models.Schema{
			Classes: filteredClasses,
		},
	}, nil
}

// GetSchemaSkipAuth can never be used as a response to a user request as it
// could leak the schema to an unauthorized user, is intended to be used for
// non-user triggered processes, such as regular updates / maintenance / etc
func (h *Handler) GetSchemaSkipAuth() schema.Schema { return h.getSchema() }

func (h *Handler) getSchema() schema.Schema {
	s := h.schemaReader.ReadOnlySchema()
	return schema.Schema{
		Objects: &s,
	}
}

func (h *Handler) Nodes() []string {
	return h.clusterState.AllNames()
}

func (h *Handler) NodeName() string {
	return h.clusterState.LocalName()
}

func (h *Handler) UpdateShardStatus(ctx context.Context,
	principal *models.Principal, class, shard, status string,
) (uint64, error) {
	err := h.Authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsMetadata(class, shard)...)
	if err != nil {
		return 0, err
	}

	return h.schemaManager.UpdateShardStatus(ctx, class, shard, status)
}

func (h *Handler) ShardsStatus(ctx context.Context,
	principal *models.Principal, class, shard string,
) (models.ShardStatusList, error) {
	err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.ShardsMetadata(class, shard)...)
	if err != nil {
		return nil, err
	}

	return h.schemaReader.GetShardsStatus(class, shard)
}

// JoinNode adds the given node to the cluster.
// Node needs to reachable via memberlist/gossip.
// If nodePort is an empty string, nodePort will be the default raft port.
// If the node is not reachable using memberlist, an error is returned
// If joining the node fails, an error is returned.
func (h *Handler) JoinNode(ctx context.Context, node string, nodePort string, voter bool) error {
	nodeAddr, ok := h.clusterState.NodeHostname(node)
	if !ok {
		return fmt.Errorf("could not resolve addr for node id %v", node)
	}
	nodeAddr = strings.Split(nodeAddr, ":")[0]

	if nodePort == "" {
		nodePort = fmt.Sprintf("%d", config.DefaultRaftPort)
	}

	if err := h.schemaManager.Join(ctx, node, nodeAddr+":"+nodePort, voter); err != nil {
		return fmt.Errorf("node failed to join cluster: %w", err)
	}
	return nil
}

// RemoveNode removes the given node from the cluster.
func (h *Handler) RemoveNode(ctx context.Context, node string) error {
	if err := h.schemaManager.Remove(ctx, node); err != nil {
		return fmt.Errorf("node failed to leave cluster: %w", err)
	}
	return nil
}

// Statistics is used to return a map of various internal stats. This should only be used for informative purposes or debugging.
func (h *Handler) Statistics() map[string]any {
	return h.schemaManager.Stats()
}

func (h *Handler) StoreSchemaV1() error {
	return h.schemaManager.StoreSchemaV1()
}
