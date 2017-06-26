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
	"github.com/weaviate/weaviate/connectors/datastore"
	"github.com/weaviate/weaviate/connectors/memory"
	"github.com/weaviate/weaviate/connectors/utils"
)

// DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	Connect() error
	Init() error
	Add(connector_utils.DatabaseObject) (string, error)
	Get(string) (connector_utils.DatabaseObject, error)
	List(string, int, int, *connector_utils.ObjectReferences) (connector_utils.DatabaseObjects, int64, error)
	ValidateKey(string) ([]connector_utils.DatabaseUsersObject, error)
	AddKey(string, connector_utils.DatabaseUsersObject) (connector_utils.DatabaseUsersObject, error)
	GetName() string
	SetConfig(interface{})
}

// GetAllConnectors contains all available connectors
func GetAllConnectors() []DatabaseConnector {
	// Set all existing connectors
	connectors := []DatabaseConnector{
		&datastore.Datastore{},
		&memory.Memory{},
	}

	return connectors
}
