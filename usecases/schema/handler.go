//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/cloud/store"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var ErrNotFound = errors.New("not found")

type metaWriter interface {
	AddClass(cls *models.Class, ss *sharding.State) error
	RestoreClass(cls *models.Class, ss *sharding.State) error
	UpdateClass(cls *models.Class, ss *sharding.State) error
	DeleteClass(name string) error
	AddProperty(class string, p *models.Property) error
	UpdateShardStatus(class, shard, status string) error
	AddTenants(class string, req *command.AddTenantsRequest) error
	UpdateTenants(class string, req *command.UpdateTenantsRequest) error
	DeleteTenants(class string, req *command.DeleteTenantsRequest) error
}

type metaReader interface {
	ClassEqual(name string) string
	// MultiTenancy checks for multi-tenancy support
	MultiTenancy(class string) bool
	ClassInfo(class string) (ci store.ClassInfo)
	ReadOnlyClass(class string) *models.Class
	ReadOnlySchema() models.Schema
	CopyShardingState(class string) *sharding.State
	ShardReplicas(class, shard string) ([]string, error)
	ShardFromUUID(class string, uuid []byte) string
	ShardOwner(class, shard string) (string, error)
	TenantShard(class, tenant string) (string, string)
	Read(class string, reader func(*models.Class, *sharding.State) error) error
	GetShardsStatus(class string) (models.ShardStatusList, error)
}

type validator interface {
	ValidateVectorIndexConfigUpdate(ctx context.Context,
		old, updated schema.VectorIndexConfig) error
	ValidateInvertedIndexConfigUpdate(ctx context.Context,
		old, updated *models.InvertedIndexConfig) error
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
	hnswConfigParser        VectorConfigParser
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
	hnswConfigParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	scaleoutManager scaleOut,
) (Handler, error) {
	handler := Handler{
		config:                  config,
		metaWriter:              store,
		metaReader:              metaReader,
		parser:                  Parser{clusterState: clusterState, hnswConfigParser: hnswConfigParser},
		validator:               validator,
		logger:                  logger,
		Authorizer:              authorizer,
		hnswConfigParser:        hnswConfigParser,
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
) error {
	err := h.Authorizer.Authorize(principal, "update",
		fmt.Sprintf("schema/%s/shards/%s", class, shard))
	if err != nil {
		return err
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
