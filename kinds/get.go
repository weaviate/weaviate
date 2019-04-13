package kinds

import (
	"context"
	"errors"

	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	utils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/go-openapi/strfmt"
)

type getRepo interface {
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error

	ListThings(ctx context.Context, limit int, wheres []*connutils.WhereQuery,
		thingsResponse *models.ThingsListResponse) error
	ListActions(ctx context.Context, limit int, wheres []*connutils.WhereQuery,
		actionsResponse *models.ActionsListResponse) error
}

// GetThing Class from the connected DB
func (m *Manager) GetThing(ctx context.Context, id strfmt.UUID) (*models.Thing, error) {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return nil, newErrInternal("could not get lock: %v", err)
	}

	defer unlock(dbLock)
	dbConnector := dbLock.Connector()

	return m.getThingFromRepo(ctx, id, dbConnector)
}

// GetThings Class from the connected DB
func (m *Manager) GetThings(ctx context.Context, limit int) ([]*models.Thing, error) {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return nil, newErrInternal("could not get lock: %v", err)
	}

	defer unlock(dbLock)
	dbConnector := dbLock.Connector()

	return m.getThingsFromRepo(ctx, limit, dbConnector)
}

// GetAction Class from connected DB
func (m *Manager) GetAction(ctx context.Context, id strfmt.UUID) (*models.Action, error) {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return nil, newErrInternal("could not get lock: %v", err)
	}

	defer unlock(dbLock)
	dbConnector := dbLock.Connector()

	return m.getActionFromRepo(ctx, id, dbConnector)
}

// GetActions Class from connected DB
func (m *Manager) GetActions(ctx context.Context, limit int) ([]*models.Action, error) {
	dbLock, err := m.db.ConnectorLock()
	if err != nil {
		return nil, newErrInternal("could not get lock: %v", err)
	}

	defer unlock(dbLock)
	dbConnector := dbLock.Connector()

	return m.getActionsFromRepo(ctx, limit, dbConnector)
}

func (m *Manager) getThingFromRepo(ctx context.Context, id strfmt.UUID,
	repo getRepo) (*models.Thing, error) {
	thing := models.Thing{}
	thing.Schema = map[string]models.JSONObject{}
	err := repo.GetThing(ctx, id, &thing)
	if err != nil {
		switch err {
		// TODO: Don't depend on utils package
		case errors.New(utils.StaticThingNotFound):
			return nil, newErrNotFound(err.Error())
		default:
			return nil, newErrInternal("could not get thing from db: %v", err)
		}
	}

	return &thing, nil
}

func (m *Manager) getThingsFromRepo(ctx context.Context, limit int,
	repo getRepo) ([]*models.Thing, error) {
	thingsResponse := models.ThingsListResponse{}
	thingsResponse.Things = []*models.Thing{}
	err := repo.ListThings(ctx, limit, []*connutils.WhereQuery{}, &thingsResponse)
	if err != nil {
		return nil, newErrInternal("could not list things: %v", err)
	}

	return thingsResponse.Things, nil
}

func (m *Manager) getActionFromRepo(ctx context.Context, id strfmt.UUID,
	repo getRepo) (*models.Action, error) {
	action := models.Action{}
	action.Schema = map[string]models.JSONObject{}
	err := repo.GetAction(ctx, id, &action)
	if err != nil {
		switch err {
		// TODO: Don't depend on utils package
		case errors.New(utils.StaticActionNotFound):
			return nil, newErrNotFound(err.Error())
		default:
			return nil, newErrInternal("could not get action from db: %v", err)
		}
	}

	return &action, nil
}

func (m *Manager) getActionsFromRepo(ctx context.Context, limit int,
	repo getRepo) ([]*models.Action, error) {
	actionsResponse := models.ActionsListResponse{}
	actionsResponse.Actions = []*models.Action{}
	err := repo.ListActions(ctx, limit, []*connutils.WhereQuery{}, &actionsResponse)
	if err != nil {
		return nil, newErrInternal("could not list actions: %v", err)
	}

	return actionsResponse.Actions, nil
}
