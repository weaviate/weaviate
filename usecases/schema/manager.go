//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/sirupsen/logrus"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	migrator            migrate.Migrator
	repo                Repo
	state               State
	callbacks           []func(updatedSchema schema.Schema)
	logger              logrus.FieldLogger
	authorizer          authorizer
	config              config.Config
	vectorizerValidator VectorizerValidator
	moduleConfig        ModuleConfig
	sync.Mutex

	hnswConfigParser VectorConfigParser
}

type VectorConfigParser func(in interface{}) (schema.VectorIndexConfig, error)

type SchemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
}

type VectorizerValidator interface {
	ValidateVectorizer(moduleName string) error
}

type ModuleConfig interface {
	SetClassDefaults(class *models.Class)
	ValidateClass(ctx context.Context, class *models.Class) error
}

// Repo describes the requirements the schema manager has to a database to load
// and persist the schema state
type Repo interface {
	SaveSchema(ctx context.Context, schema State) error

	// should return nil (and no error) to indicate that no remote schema had
	// been stored before
	LoadSchema(ctx context.Context) (*State, error)
}

// NewManager creates a new manager
func NewManager(migrator migrate.Migrator, repo Repo,
	logger logrus.FieldLogger, authorizer authorizer, config config.Config,
	hnswConfigParser VectorConfigParser, vectorizerValidator VectorizerValidator,
	moduleConfig ModuleConfig) (*Manager, error) {
	m := &Manager{
		config:              config,
		migrator:            migrator,
		repo:                repo,
		state:               State{},
		logger:              logger,
		authorizer:          authorizer,
		hnswConfigParser:    hnswConfigParser,
		vectorizerValidator: vectorizerValidator,
		moduleConfig:        moduleConfig,
	}

	err := m.loadOrInitializeSchema(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not laod or initialize schema: %v", err)
	}

	return m, nil
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ObjectSchema *models.Schema `json:"object"`
}

// SchemaFor a specific kind
func (s *State) SchemaFor() *models.Schema {
	return s.ObjectSchema
}

func (m *Manager) saveSchema(ctx context.Context) error {
	m.logger.
		WithField("action", "schema_update").
		Debug("saving updated schema to configuration store")

	err := m.repo.SaveSchema(ctx, m.state)
	if err != nil {
		return err
	}

	m.TriggerSchemaUpdateCallbacks()
	return nil
}

// RegisterSchemaUpdateCallback allows other usecases to register a primitive
// type update callback. The callbacks will be called any time we persist a
// schema upadate
func (m *Manager) RegisterSchemaUpdateCallback(callback func(updatedSchema schema.Schema)) {
	m.callbacks = append(m.callbacks, callback)
}

func (m *Manager) TriggerSchemaUpdateCallbacks() {
	schema := m.GetSchemaSkipAuth()

	for _, cb := range m.callbacks {
		cb(schema)
	}
}

func (m *Manager) loadOrInitializeSchema(ctx context.Context) error {
	schema, err := m.repo.LoadSchema(ctx)
	if err != nil {
		return fmt.Errorf("could not load schema:  %v", err)
	}

	if schema == nil {
		schema = newSchema()
	}

	if err := m.parseConfigs(ctx, schema); err != nil {
		return errors.Wrap(err, "load schema")
	}

	// store in local cache
	m.state = *schema

	// store in remote repo
	if err := m.repo.SaveSchema(ctx, m.state); err != nil {
		return fmt.Errorf("initialized a new schema, but couldn't update remote: %v", err)
	}

	return nil
}

func newSchema() *State {
	return &State{
		ObjectSchema: &models.Schema{
			Classes: []*models.Class{},
		},
	}
}

func (m *Manager) parseConfigs(ctx context.Context, schema *State) error {
	for _, class := range schema.ObjectSchema.Classes {
		if err := m.parseVectorIndexConfig(ctx, class); err != nil {
			return errors.Wrapf(err, "class %s: vector index config", class.Class)
		}

		if err := m.parseShardingConfig(ctx, class); err != nil {
			return errors.Wrapf(err, "class %s: sharding config", class.Class)
		}
	}

	return nil
}
