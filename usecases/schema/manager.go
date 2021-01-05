//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/locks"
	"github.com/semi-technologies/weaviate/usecases/schema/migrate"
	"github.com/sirupsen/logrus"
)

// Manager Manages schema changes at a use-case level, i.e. agnostic of
// underlying databases or storage providers
type Manager struct {
	migrator         migrate.Migrator
	repo             Repo
	stopwordDetector stopwordDetector
	c11yClient       c11yClient
	locks            locks.ConnectorSchemaLock
	state            State
	callbacks        []func(updatedSchema schema.Schema)
	logger           logrus.FieldLogger
	authorizer       authorizer
}

type SchemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
}

// Repo describes the requirements the schema manager has to a database to load
// and persist the schema state
type Repo interface {
	SaveSchema(ctx context.Context, schema State) error

	// should return nil (and no error) to indicate that no remote schema had
	// been stored before
	LoadSchema(ctx context.Context) (*State, error)
}

type stopwordDetector interface {
	IsStopWord(ctx context.Context, word string) (bool, error)
}

type c11yClient interface {
	IsWordPresent(ctx context.Context, word string) (bool, error)
}

// NewManager creates a new manager
func NewManager(migrator migrate.Migrator, repo Repo, locks locks.ConnectorSchemaLock,
	logger logrus.FieldLogger, c11yClient c11yClient,
	authorizer authorizer, swd stopwordDetector) (*Manager, error) {
	m := &Manager{
		migrator:         migrator,
		repo:             repo,
		locks:            locks,
		state:            State{},
		logger:           logger,
		stopwordDetector: swd,
		authorizer:       authorizer,
		c11yClient:       c11yClient,
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
func (s *State) SchemaFor(k kind.Kind) *models.Schema {
	switch k {
	case kind.Object:
		return s.ObjectSchema
	default:
		// It is fine to panic here, as this indicates an unrecoverable error in
		// the program, rather than an invalid input based on user input
		panic(fmt.Sprintf("Passed wrong object, but %v", k))
	}
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
			Type:    "object",
		},
	}
}

// TODO: this sholud be part of the text2vec-contextionary module
// VectorizeClassName is the only safe way to access this property, as it could
// otherwise be nil. It is also the single place a default is set
func VectorizeClassName(class *models.Class) bool {
	if class.Vectorizer != config.VectorizerModuleText2VecContextionary {
		// this class doesn't use the text2-vec module, so it can never be true
		return false
	}

	const defaultValue = true

	// TODO: This is not the right place to parse module config, as part of the
	// modularziation this should be done in a different place. However, for now,
	// with only one hard-coded module, this is helpful:
	moduleConfig, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		return defaultValue
	}

	t2vConfig, ok := moduleConfig["text2vec-contextionary"]
	if !ok {
		return defaultValue
	}

	asMap, ok := t2vConfig.(map[string]interface{})
	if !ok {
		return defaultValue
	}

	vCN, ok := asMap["vectorizeClassName"]
	if !ok {
		return defaultValue
	}

	asBool, ok := vCN.(bool)
	if !ok {
		return defaultValue
	}

	return asBool
}
