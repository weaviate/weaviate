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

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	graphqlapiLocal "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local"
	batchmodels "github.com/creativesoftwarefdn/weaviate/adapters/handlers/rest/batch/models"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
)

// BaseConnector is the interface that all connectors should have
type BaseConnector interface {
	schema_migrator.Migrator

	Connect() error
	Init(ctx context.Context) error
	SetServerAddress(serverAddress string)
	SetSchema(s schema.Schema)
	SetLogger(l logrus.FieldLogger)

	AddThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	AddThingsBatch(ctx context.Context, things batchmodels.Things) error
	GetThing(ctx context.Context, UUID strfmt.UUID, thingResponse *models.Thing) error
	ListThings(ctx context.Context, limit int, thingsResponse *models.ThingsListResponse) error
	UpdateThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error

	AddAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error
	AddActionsBatch(ctx context.Context, things batchmodels.Actions) error
	GetAction(ctx context.Context, UUID strfmt.UUID, actionResponse *models.Action) error
	ListActions(ctx context.Context, limit int, actionsResponse *models.ActionsListResponse) error
	UpdateAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error
	DeleteAction(ctx context.Context, action *models.Action, UUID strfmt.UUID) error

	AddBatchReferences(ctx context.Context, references batchmodels.References) error
}

// DatabaseConnector is the interface that all DB-connectors should have
type DatabaseConnector interface {
	BaseConnector
	connector_state.Connector
	graphqlapiLocal.Resolver

	GetThings(ctx context.Context, UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error
	GetActions(ctx context.Context, UUIDs []strfmt.UUID, actionResponse *models.ActionsListResponse) error
}
