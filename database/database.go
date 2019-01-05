/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
package database

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/messages"
)

type Database interface {
	graphqlapi.DatabaseResolverProvider

	ConnectorLock() ConnectorLock
	SchemaLock() SchemaLock
}

// A database consists of a schema manager and a connector.
// The schema manager ensures that all weaviate instances (or just the one) agree on which schema is used.
// The connector talks to the backing data store to persist the actual data.
type database struct {
	locker    RWLocker
	manager   SchemaManager
	connector dbconnector.DatabaseConnector
}

func New(messaging *messages.Messaging, locker RWLocker, manager SchemaManager, connector dbconnector.DatabaseConnector, context contextionary.Contextionary) (error, Database) {
	// Link the manager and connector
	manager.SetStateConnector(connector)
	connector.SetStateManager(manager)

	// Set updates to the schema.
	manager.RegisterSchemaUpdateCallback(func(updatedSchema schema.Schema) {
		connector.SetSchema(updatedSchema)
	})

	initialState := manager.GetInitialConnectorState()

	connector.SetState(initialState)
	connector.SetSchema(manager.GetSchema())
	connector.SetMessaging(messaging)

	// Make the connector try to connect to a database
	errConnect := connector.Connect()
	if errConnect != nil {
		messaging.ExitError(1, fmt.Sprintf("Could not connect to backing database: %s", errConnect.Error()))
	}

	// Init the database. Manually lock the schema, so that initialization happens atomically across all instances.
	locker.Lock()
	errInit := connector.Init()
	locker.Unlock()
	if errInit != nil {
		messaging.ExitError(1, fmt.Sprintf("Could not initialize connector: %s", errInit.Error()))
	}

	manager.SetContextionary(context)

	return nil, &database{
		locker:    locker,
		manager:   manager,
		connector: connector,
	}
}

// Get a lock on the connector, allow access to the database, cannot modify schema.
func (db *database) ConnectorLock() ConnectorLock {
	db.locker.RLock()

	return &connectorLock{
		db:    db,
		valid: true,
	}
}

// Get a lock on the schema manager. Can both modify the database and the schema.
// We ensure that only request in one instance should hold this lock at the same time.
func (db *database) SchemaLock() SchemaLock {
	db.locker.Lock()

	return &schemaLock{
		db:    db,
		valid: true,
	}
}

func (db *database) GetResolver() graphqlapi.ClosingResolver {
	lock := db.ConnectorLock()

	return &dbClosingResolver{
		connectorLock: lock,
	}
}
