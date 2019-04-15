package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// ValidateThing without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateThing(ctx context.Context, class *models.Thing) error {
	lock, err := m.db.ConnectorLock()
	if err != nil {
		return newErrInternal("could not acquire lock: %v", err)
	}
	defer unlock(lock)
	dbConnector := lock.Connector()
	schema := lock.GetSchema()

	err = m.validateThing(ctx, schema, class, dbConnector)
	if err != nil {
		return newErrInvalidUserInput("invalid thing: %v", err)
	}

	return nil
}

// ValidateAction without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateAction(ctx context.Context, class *models.Action) error {
	lock, err := m.db.ConnectorLock()
	if err != nil {
		return newErrInternal("could not acquire lock: %v", err)
	}
	defer unlock(lock)
	dbConnector := lock.Connector()
	schema := lock.GetSchema()

	err = m.validateAction(ctx, schema, class, dbConnector)
	if err != nil {
		return newErrInvalidUserInput("invalid action: %v", err)
	}

	return nil
}
