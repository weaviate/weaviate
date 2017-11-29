/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package dbconnector

import (
	"github.com/go-openapi/strfmt"
	"github.com/creativesoftwarefdn/weaviate/connectors/utils"

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
	ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error
	UpdateThing(thing *models.Thing, UUID strfmt.UUID) error
	DeleteThing(UUID strfmt.UUID) error

	AddAction(action *models.Action, UUID strfmt.UUID) error
	GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error
	ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error
	UpdateAction(action *models.Action, UUID strfmt.UUID) error
	DeleteAction(UUID strfmt.UUID) error

	AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error
	ValidateToken(UUID strfmt.UUID, key *models.KeyTokenGetResponse) error
	GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error
	DeleteKey(UUID strfmt.UUID) error
	GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error
}

// CacheConnector is the interface that all cache-connectors should have
type CacheConnector interface {
	DatabaseConnector

	SetDatabaseConnector(dbConnector DatabaseConnector)
}
