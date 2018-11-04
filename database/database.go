package database

import (
	"fmt"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/messages"
)

type Database interface {
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

func New(messaging *messages.Messaging, locker RWLocker, manager SchemaManager, connector dbconnector.DatabaseConnector) (error, Database) {
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

	// TODO: probably needs to go. We're not using address anymore.
	//connector.SetServerAddress(serverConfig.GetHostAddress())

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
