package database

import (
	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
)

// A database consists of a schema manager and a connector.
// The schema manager ensures that all weaviate instances (or just the one) agree on which schema is used.
// The connector talks to the backing data store to persist the actual data.
type Database struct {
	locker    *RWLocker
	manager   *SchemaManager
	connector *dbconnector.DatabaseConnector
}

func New(locker *RWLocker, manager *SchemaManager, connector *dbconnector.DatabaseConnector) *Database {
	return &Database{
		locker:    locker,
		manager:   manager,
		connector: connector,
	}
}

// Get a lock on the connector, allow access to the database, cannot modify schema.
func (db *Database) ConnectorLock() *ConnectorLock {
	(*db.locker).RLock()

	return &ConnectorLock{
		db:    db,
		valid: true,
	}
}

// Get a lock on the schema manager. Can both modify the database and the schema.
// We ensure that only request in one instance should hold this lock at the same time.
func (db *Database) SchemaLock() *SchemaLock {
	(*db.locker).Lock()

	return &SchemaLock{
		db:    db,
		valid: true,
	}
}
