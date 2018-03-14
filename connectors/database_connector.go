/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package dbconnector

import (
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"
	"github.com/go-openapi/strfmt"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/schema"
)

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Init() error
	GetName() string
	SetServerAddress(serverAddress string)
	SetConfig(configInput *config.Environment) error
	SetSchema(schemaInput *schema.WeaviateSchema) error
	SetMessaging(m *messages.Messaging) error

	AddThing(thing *models.Thing, UUID strfmt.UUID) error
	GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error
	GetThings(UUIDs []strfmt.UUID, thingResponse *models.ThingsListResponse) error
	ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error
	UpdateThing(thing *models.Thing, UUID strfmt.UUID) error
	DeleteThing(thing *models.Thing, UUID strfmt.UUID) error
	HistoryThing(UUID strfmt.UUID, history *models.ThingHistory) error
	MoveToHistoryThing(thing *models.Thing, UUID strfmt.UUID, deleted bool) error

	AddAction(action *models.Action, UUID strfmt.UUID) error
	GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error
	ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error
	UpdateAction(action *models.Action, UUID strfmt.UUID) error
	DeleteAction(action *models.Action, UUID strfmt.UUID) error
	HistoryAction(UUID strfmt.UUID, history *models.ActionHistory) error
	MoveToHistoryAction(action *models.Action, UUID strfmt.UUID, deleted bool) error

	AddKey(key *models.Key, UUID strfmt.UUID, token string) error
	ValidateToken(UUID strfmt.UUID, key *models.KeyGetResponse) (token string, err error)
	GetKey(UUID strfmt.UUID, keyResponse *models.KeyGetResponse) error
	DeleteKey(key *models.Key, UUID strfmt.UUID) error
	GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyGetResponse) error
	UpdateKey(key *models.Key, UUID strfmt.UUID, token string) error
}

// CacheConnector is the interface that all cache-connectors should have
type CacheConnector interface {
	DatabaseConnector

	SetDatabaseConnector(dbConnector DatabaseConnector)
}
