package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/validation"
	"github.com/go-openapi/strfmt"
)

type addRepo interface {
	// The validation package needs to be able to get existing classes as well
	getRepo

	// methods to add new items
	AddAction(ctx context.Context, class *models.Action, id strfmt.UUID) error
	AddThing(ctx context.Context, class *models.Thing, id strfmt.UUID) error
}

// AddAction Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddAction(ctx context.Context, class *models.Action) (*models.Action, error) {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	dbConnector := schemaLock.Connector()
	schemaManager := schemaLock.SchemaManager()

	return m.addActionToConnectorAndSchema(ctx, class, dbConnector, schemaManager)
}

func (m *Manager) addActionToConnectorAndSchema(ctx context.Context, class *models.Action,
	addRepo addRepo, schemaManager database.SchemaManager) (*models.Action, error) {
	class.ID = generateUUID()
	err := m.validateAction(ctx, schemaManager.GetSchema(), class, addRepo)
	if err != nil {
		return nil, newErrInvalidUserInput("invalid action: %v", err)
	}

	err = m.addNetworkDataTypesForAction(ctx, schemaManager, class)
	if err != nil {
		return nil, newErrInternal("could not update schema for network refs: %v", err)
	}

	addRepo.AddAction(ctx, class, class.ID)
	if err != nil {
		return nil, newErrInternal("could not store action: %v", err)
	}

	return class, nil
}

func (m *Manager) validateAction(ctx context.Context, dbschema schema.Schema,
	class *models.Action, getRepo getRepo) error {
	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(dbschema)
	return validation.ValidateActionBody(
		ctx, class, databaseSchema, getRepo, m.network, m.config)
}

// AddThing Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddThing(ctx context.Context, class *models.Thing) (*models.Thing, error) {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	dbConnector := schemaLock.Connector()
	schemaManager := schemaLock.SchemaManager()

	return m.addThingToConnectorAndSchema(ctx, class, dbConnector, schemaManager)
}

func (m *Manager) addThingToConnectorAndSchema(ctx context.Context, class *models.Thing,
	addRepo addRepo, schemaManager database.SchemaManager) (*models.Thing, error) {
	class.ID = generateUUID()
	err := m.validateThing(ctx, schemaManager.GetSchema(), class, addRepo)
	if err != nil {
		return nil, newErrInvalidUserInput("invalid thing: %v", err)
	}

	err = m.addNetworkDataTypesForThing(ctx, schemaManager, class)
	if err != nil {
		return nil, newErrInternal("could not update schema for network refs: %v", err)
	}

	addRepo.AddThing(ctx, class, class.ID)
	if err != nil {
		return nil, newErrInternal("could not store thing: %v", err)
	}

	return class, nil
}

func (m *Manager) validateThing(ctx context.Context, dbschema schema.Schema,
	class *models.Thing, getRepo getRepo) error {
	// Validate schema given in body with the weaviate schema
	databaseSchema := schema.HackFromDatabaseSchema(dbschema)
	return validation.ValidateThingBody(
		ctx, class, databaseSchema, getRepo, m.network, m.config)
}

func (m *Manager) addNetworkDataTypesForThing(ctx context.Context, sm database.SchemaManager, class *models.Thing) error {
	refSchemaUpdater := newReferenceSchemaUpdater(ctx, sm, m.network, class.Class, kind.THING_KIND)
	return refSchemaUpdater.addNetworkDataTypes(class.Schema)
}

func (m *Manager) addNetworkDataTypesForAction(ctx context.Context, sm database.SchemaManager, class *models.Action) error {
	refSchemaUpdater := newReferenceSchemaUpdater(ctx, sm, m.network, class.Class, kind.ACTION_KIND)
	return refSchemaUpdater.addNetworkDataTypes(class.Schema)
}
