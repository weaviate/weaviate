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
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/store"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var ErrNotFound = errors.New("not found")

type metaWriter interface {
	// Schema writes operation
	AddClass(cls *models.Class, ss *sharding.State) (uint64, error)
	RestoreClass(cls *models.Class, ss *sharding.State) (uint64, error)
	UpdateClass(cls *models.Class, ss *sharding.State) (uint64, error)
	DeleteClass(name string) (uint64, error)
	AddProperty(class string, p ...*models.Property) (uint64, error)
	UpdateShardStatus(class, shard, status string) (uint64, error)
	AddTenants(class string, req *command.AddTenantsRequest) (uint64, error)
	UpdateTenants(class string, req *command.UpdateTenantsRequest) (uint64, error)
	DeleteTenants(class string, req *command.DeleteTenantsRequest) (uint64, error)

	// Strongly consistent schema read. These endpoints will emit a query to the leader to ensure that the data is read
	// from an up to date schema.
	QueryReadOnlyClass(name string) (*models.Class, uint64, error)
	QuerySchema() (models.Schema, error)
	QueryTenants(class string) ([]*models.Tenant, uint64, error)
	QueryShardOwner(class, shard string) (string, uint64, error)
	QueryTenantsShards(class string, tenants ...string) (map[string]string, error)

	// Cluster related operations
	Join(_ context.Context, nodeID, raftAddr string, voter bool) error
	Remove(_ context.Context, nodeID string) error
	Stats() map[string]any
	StoreSchemaV1() error
}

type metaReader interface {
	ClassEqual(name string) string
	// MultiTenancy checks for multi-tenancy support
	MultiTenancy(class string) models.MultiTenancyConfig
	ClassInfo(class string) (ci store.ClassInfo)
	// ReadOnlyClass return class model.
	ReadOnlyClass(name string) *models.Class
	ReadOnlySchema() models.Schema
	CopyShardingState(class string) *sharding.State
	ShardReplicas(class, shard string) ([]string, error)
	ShardFromUUID(class string, uuid []byte) string
	ShardOwner(class, shard string) (string, error)
	Read(class string, reader func(*models.Class, *sharding.State) error) error
	GetShardsStatus(class string) (models.ShardStatusList, error)

	// WithVersion endpoints return the data with the schema version
	ClassInfoWithVersion(ctx context.Context, class string, version uint64) (store.ClassInfo, error)
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
	metaWriter metaWriter
	metaReader metaReader
	validator  validator

	logger                  logrus.FieldLogger
	Authorizer              authorizer
	config                  config.Config
	vectorizerValidator     VectorizerValidator
	moduleConfig            ModuleConfig
	clusterState            clusterState
	configParser            VectorConfigParser
	invertedConfigValidator InvertedConfigValidator
	scaleOut                scaleOut
	parser                  Parser
}

// NewHandler creates a new handler
func NewHandler(
	store metaWriter,
	metaReader metaReader,
	validator validator,
	logger logrus.FieldLogger, authorizer authorizer, config config.Config,
	configParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	scaleoutManager scaleOut,
) (Handler, error) {
	handler := Handler{
		config:                  config,
		metaWriter:              store,
		metaReader:              metaReader,
		parser:                  Parser{clusterState: clusterState, configParser: configParser, validator: validator},
		validator:               validator,
		logger:                  logger,
		Authorizer:              authorizer,
		configParser:            configParser,
		vectorizerValidator:     vectorizerValidator,
		invertedConfigValidator: invertedConfigValidator,
		moduleConfig:            moduleConfig,
		clusterState:            clusterState,
		scaleOut:                scaleoutManager,
	}

	handler.scaleOut.SetSchemaManager(metaReader)

	return handler, nil
}

// GetSchema retrieves a locally cached copy of the schema
func (h *Handler) GetSchema(principal *models.Principal) (schema.Schema, error) {
	err := h.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return schema.Schema{}, err
	}

	return h.getSchema(), nil
}

// GetSchema retrieves a locally cached copy of the schema
func (m *Handler) GetConsistentSchema(principal *models.Principal, consistency bool) (schema.Schema, error) {
	if err := m.Authorizer.Authorize(principal, "list", "schema/*"); err != nil {
		return schema.Schema{}, err
	}

	if !consistency {
		return m.getSchema(), nil
	}

	if consistentSchema, err := m.metaWriter.QuerySchema(); err != nil {
		return schema.Schema{}, fmt.Errorf("could not read schema with strong consistency: %w", err)
	} else {
		return schema.Schema{
			Objects: &consistentSchema,
		}, nil
	}
}

// GetSchemaSkipAuth can never be used as a response to a user request as it
// could leak the schema to an unauthorized user, is intended to be used for
// non-user triggered processes, such as regular updates / maintenance / etc
func (h *Handler) GetSchemaSkipAuth() schema.Schema { return h.getSchema() }

func (h *Handler) getSchema() schema.Schema {
	s := h.metaReader.ReadOnlySchema()
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
	err := h.Authorizer.Authorize(principal, "update",
		fmt.Sprintf("schema/%s/shards/%s", class, shard))
	if err != nil {
		return 0, err
	}

	return h.metaWriter.UpdateShardStatus(class, shard, status)
}

func (h *Handler) ShardsStatus(ctx context.Context,
	principal *models.Principal, class string,
) (models.ShardStatusList, error) {
	err := h.Authorizer.Authorize(principal, "list", fmt.Sprintf("schema/%s/shards", class))
	if err != nil {
		return nil, err
	}

	return h.metaReader.GetShardsStatus(class)
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

	return h.metaWriter.Join(ctx, node, nodeAddr+":"+nodePort, voter)
}

// RemoveNode removes the given node from the cluster.
func (h *Handler) RemoveNode(ctx context.Context, node string) error {
	return h.metaWriter.Remove(ctx, node)
}

// Statistics is used to return a map of various internal stats. This should only be used for informative purposes or debugging.
func (h *Handler) Statistics() map[string]any {
	return h.metaWriter.Stats()
}

func (h *Handler) StoreSchemaV1() error {
	return h.metaWriter.StoreSchemaV1()
}
