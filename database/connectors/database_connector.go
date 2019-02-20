/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package dbconnector

import (
	"context"

	connutils "github.com/creativesoftwarefdn/weaviate/database/utils"
	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	graphqlapiLocal "github.com/creativesoftwarefdn/weaviate/graphqlapi/local"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	batchmodels "github.com/creativesoftwarefdn/weaviate/restapi/batch/models"
)

// BaseConnector is the interface that all connectors should have
type BaseConnector interface {
	schema_migrator.Migrator

	Connect() error
	Init(ctx context.Context) error
	SetServerAddress(serverAddress string)
	SetSchema(s schema.Schema)
	SetMessaging(m *messages.Messaging)

	AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	AddThingsBatch(ctx context.Context, things batchmodels.Things) error
	GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error
	ListThings(ctx context.Context, first int, offset int, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error
	UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	HistoryThing(ctx context.Context, UUID strfmt.UUID, history *models.ThingHistory) error
	MoveToHistoryThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID, deleted bool) error

	AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error
	AddActionsBatch(ctx context.Context, things batchmodels.Actions) error
	GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error
	ListActions(ctx context.Context, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error
	UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error
	DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error
	HistoryAction(ctx context.Context, UUID strfmt.UUID, history *models.ActionHistory) error
	MoveToHistoryAction(ctx context.Context, action *models.Action, UUID strfmt.UUID, deleted bool) error
}

// DatabaseConnector is the interface that all DB-connectors should have
type DatabaseConnector interface {
	BaseConnector
	connector_state.Connector
	graphqlapiLocal.Resolver

	GetThings(ctx context.Context, UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error
	GetActions(ctx context.Context, UUIDs []strfmt.UUID, actionResponse *models.ActionsListResponse) error
}
