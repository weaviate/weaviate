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
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	store "github.com/weaviate/weaviate/cloud/store"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var ErrNotFound = errors.New("not found")

type metaWriter interface {
	AddClass(cls *models.Class, ss *sharding.State) error
	// RestoreClass(cls *models.Class, ss *sharding.State) error
	UpdateClass(cls *models.Class, ss *sharding.State) error
	DeleteClass(name string) error

	AddProperty(class string, p *models.Property) error
	AddTenants(class string, req *command.AddTenantsRequest) error
	UpdateTenants(class string, req *command.UpdateTenantsRequest) error
	DeleteTenants(class string, req *command.DeleteTenantsRequest) error
}

type metaReader interface {
	// MultiTenancy checks for multi-tenancy support
	ClassEqual(name string) string
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
}

// The handler manages API requests for manipulating class schemas.
// This separation of responsibilities helps decouple these tasks
// from the Manager class, which combines many unrelated functions.
// By delegating these clear responsibilities to the handler, it maintains
// a clean separation from the manager, enhancing code modularity and maintainability.

type Handler struct {
	metaWriter metaWriter
	metaReader metaReader
	migrator   Migrator

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

// NewHandler creates a new manager
func NewHandler(
	store metaWriter,
	metaReader metaReader,
	migrator Migrator,
	logger logrus.FieldLogger, authorizer authorizer, config config.Config,
	hnswConfigParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	invertedConfigValidator InvertedConfigValidator,
	moduleConfig ModuleConfig, clusterState clusterState,
	scaleoutManager scaleOut,
) (Handler, error) {
	m := Handler{
		config:     config,
		metaWriter: store,
		metaReader: metaReader,
		parser:     Parser{clusterState: clusterState, hnswConfigParser: hnswConfigParser},

		migrator:                migrator,
		logger:                  logger,
		Authorizer:              authorizer,
		hnswConfigParser:        hnswConfigParser,
		vectorizerValidator:     vectorizerValidator,
		invertedConfigValidator: invertedConfigValidator,
		moduleConfig:            moduleConfig,
		clusterState:            clusterState,
		scaleOut:                scaleoutManager,
	}

	// TODO-RAFT
	// m.scaleOut.SetSchemaManager(m)

	return m, nil
}

// GetSchema retrieves a locally cached copy of the schema
func (m *Handler) GetSchema(principal *models.Principal) (schema.Schema, error) {
	err := m.Authorizer.Authorize(principal, "list", "schema/*")
	if err != nil {
		return schema.Schema{}, err
	}

	return m.getSchema(), nil
}

// GetSchemaSkipAuth can never be used as a response to a user request as it
// could leak the schema to an unauthorized user, is intended to be used for
// non-user triggered processes, such as regular updates / maintenance / etc
func (m *Handler) GetSchemaSkipAuth() schema.Schema { return m.getSchema() }

func (m *Handler) getSchema() schema.Schema {
	s := m.metaReader.ReadOnlySchema()
	return schema.Schema{
		Objects: &s,
	}
}

func (m *Handler) Nodes() []string {
	return m.clusterState.AllNames()
}

func (m *Handler) NodeName() string {
	return m.clusterState.LocalName()
}
