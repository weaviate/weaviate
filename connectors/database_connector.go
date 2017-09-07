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

	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors/dgraph"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/schema"
)

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Init() error
	GetName() string
	SetConfig(*config.Environment) error
	SetSchema(*schema.WeaviateSchema) error

	AddThing(*models.Thing, strfmt.UUID) error // TODO: response: ThingGetResponse?
	GetThing(strfmt.UUID) (models.ThingGetResponse, error)
	ListThings(int, int) (models.ThingsListResponse, error)
	UpdateThing(*models.Thing, strfmt.UUID) error
	DeleteThing(strfmt.UUID) error

	AddAction(*models.Action, strfmt.UUID) error
	GetAction(strfmt.UUID) (models.ActionGetResponse, error)
	ListActions(strfmt.UUID, int, int) (models.ActionsListResponse, error)
	UpdateAction(*models.Action, strfmt.UUID) error
	DeleteAction(strfmt.UUID) error

	AddKey(*connector_utils.Key, strfmt.UUID) error
	ValidateToken(strfmt.UUID) (connector_utils.Key, error)
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
