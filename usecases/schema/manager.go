//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"
	"log"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/locks"
	"github.com/semi-technologies/weaviate/usecases/network"
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
	network          network.Network
	callbacks        []func(updatedSchema schema.Schema)
	logger           logrus.FieldLogger
	authorizer       authorizer
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
	network network.Network, logger logrus.FieldLogger, c11yClient c11yClient,
	authorizer authorizer, swd stopwordDetector) (*Manager, error) {
	m := &Manager{
		migrator:         migrator,
		repo:             repo,
		locks:            locks,
		state:            State{},
		network:          network,
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

type unlocker interface {
	Unlock() error
}

func unlock(l unlocker) {
	err := l.Unlock()
	if err != nil {
		log.Fatal(err)
	}
}

// State is a cached copy of the schema that can also be saved into a remote
// storage, as specified by Repo
type State struct {
	ActionSchema *models.Schema `json:"action"`
	ThingSchema  *models.Schema `json:"thing"`
}

// SchemaFor a specific kind
func (s *State) SchemaFor(k kind.Kind) *models.Schema {
	switch k {
	case kind.Thing:
		return s.ThingSchema
	case kind.Action:
		return s.ActionSchema
	default:
		// It is fine to panic here, as this indicates an unrecoverable error in
		// the program, rather than an invalid input based on user input
		panic(fmt.Sprintf("Passed wrong neither thing nor action, but %v", k))
	}
}

func (m *Manager) saveSchema(ctx context.Context) error {
	m.logger.
		WithField("action", "schema_update").
		WithField("configuration_store", "etcd").
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
		ActionSchema: &models.Schema{
			Classes: []*models.Class{},
			Type:    "action",
		},
		ThingSchema: &models.Schema{
			Classes: []*models.Class{},
			Type:    "thing",
		},
	}
}
