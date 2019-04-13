package kinds

import (
	"context"
	"time"

	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

type updateRepo interface {
	// The validation package needs to be able to get existing classes as well
	getRepo

	// methods to update new items
	UpdateAction(ctx context.Context, class *models.Action, id strfmt.UUID) error
	UpdateThing(ctx context.Context, class *models.Thing, id strfmt.UUID) error
}

// UpdateAction Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateAction(ctx context.Context, id strfmt.UUID, class *models.Action) (*models.Action, error) {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	dbConnector := schemaLock.Connector()
	schemaManager := schemaLock.SchemaManager()

	return m.updateActionToConnectorAndSchema(ctx, id, class, dbConnector, schemaManager)
}

func (m *Manager) updateActionToConnectorAndSchema(ctx context.Context, id strfmt.UUID,
	class *models.Action, updateRepo updateRepo, schemaManager database.SchemaManager) (*models.Action,
	error) {
	if id != class.ID {
		return nil, newErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	_, err := m.getActionFromRepo(ctx, id, updateRepo)
	if err != nil {
		return nil, err
	}

	err = m.validateAction(ctx, schemaManager.GetSchema(), class, updateRepo)
	if err != nil {
		return nil, newErrInvalidUserInput("invalid action: %v", err)
	}

	err = m.addNetworkDataTypesForAction(ctx, schemaManager, class)
	if err != nil {
		return nil, newErrInternal("could not update schema for network refs: %v", err)
	}

	class.LastUpdateTimeUnix = unixNow()
	updateRepo.UpdateAction(ctx, class, class.ID)
	if err != nil {
		return nil, newErrInternal("could not store updated action: %v", err)
	}

	return class, nil
}

// UpdateThing Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateThing(ctx context.Context, id strfmt.UUID, class *models.Thing) (*models.Thing, error) {
	schemaLock, err := m.db.SchemaLock()
	if err != nil {
		return nil, newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(schemaLock)
	dbConnector := schemaLock.Connector()
	schemaManager := schemaLock.SchemaManager()

	return m.updateThingToConnectorAndSchema(ctx, id, class, dbConnector, schemaManager)
}

func (m *Manager) updateThingToConnectorAndSchema(ctx context.Context, id strfmt.UUID,
	class *models.Thing, updateRepo updateRepo, schemaManager database.SchemaManager) (*models.Thing,
	error) {
	if id != class.ID {
		return nil, newErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	_, err := m.getThingFromRepo(ctx, id, updateRepo)
	if err != nil {
		return nil, err
	}

	err = m.validateThing(ctx, schemaManager.GetSchema(), class, updateRepo)
	if err != nil {
		return nil, newErrInvalidUserInput("invalid thing: %v", err)
	}

	err = m.addNetworkDataTypesForThing(ctx, schemaManager, class)
	if err != nil {
		return nil, newErrInternal("could not update schema for network refs: %v", err)
	}

	class.LastUpdateTimeUnix = unixNow()
	updateRepo.UpdateThing(ctx, class, class.ID)
	if err != nil {
		return nil, newErrInternal("could not store updated thing: %v", err)
	}

	return class, nil
}

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
