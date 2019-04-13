package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

type deleteRepo interface {
	// delete depends on getRepo for a not-found check
	getRepo

	// TODO: Delete unnecessary 2nd arg
	DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	DeleteAction(ctx context.Context, thing *models.Action, UUID strfmt.UUID) error
}

// DeleteAction Class Instance from the conncected DB
func (m *Manager) DeleteAction(ctx context.Context, id strfmt.UUID) error {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(dbLock)

	dbConnector := dbLock.Connector()
	return m.deleteActionFromRepo(ctx, id, dbConnector)
}

func (m *Manager) deleteActionFromRepo(ctx context.Context, id strfmt.UUID,
	repo deleteRepo) error {

	_, err := m.getActionFromRepo(ctx, id, repo)
	if err != nil {
		return err
	}

	repo.DeleteAction(ctx, nil, id)
	if err != nil {
		return newErrInternal("could not delete action: %v", err)
	}

	return nil
}

// DeleteThing Class Instance from the conncected DB
func (m *Manager) DeleteThing(ctx context.Context, id strfmt.UUID) error {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock(dbLock)

	dbConnector := dbLock.Connector()
	return m.deleteThingFromRepo(ctx, id, dbConnector)
}

func (m *Manager) deleteThingFromRepo(ctx context.Context, id strfmt.UUID,
	repo deleteRepo) error {

	_, err := m.getThingFromRepo(ctx, id, repo)
	if err != nil {
		return err
	}

	repo.DeleteThing(ctx, nil, id)
	if err != nil {
		return newErrInternal("could not delete thing: %v", err)
	}

	return nil
}
