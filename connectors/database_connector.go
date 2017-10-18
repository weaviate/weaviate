/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package dbconnector

import (
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/connectors/utils"

	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors/dgraph"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/schema"
)

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Init() error
	GetName() string
	SetServerAddress(serverAddress string)
	SetConfig(configInput *config.Environment) error
	SetSchema(schemaInput *schema.WeaviateSchema) error

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

// GetAllConnectors contains all available connectors
func GetAllConnectors() []DatabaseConnector {
	// Set all existing connectors
	connectors := []DatabaseConnector{
		&dgraph.Dgraph{},
	}

	return connectors
}

// CreateDatabaseConnector gets the database connector by name from config
func CreateDatabaseConnector(env *config.Environment) DatabaseConnector {
	// Get all connectors
	connectors := GetAllConnectors()

	// Loop through all connectors and determine its name
	for _, connector := range connectors {
		if connector.GetName() == env.Database.Name {
			return connector
		}
	}

	return nil
}
