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
package database

import (
	"fmt"
	"log"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/creativesoftwarefdn/weaviate/contextionary"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi"
	"github.com/creativesoftwarefdn/weaviate/messages"
	recipe "github.com/etcd-io/etcd/contrib/recipes"
)

type Database interface {
	graphqlapi.DatabaseResolverProvider

	ConnectorLock() (ConnectorLock, error)
	SchemaLock() (SchemaLock, error)
}

// A database consists of a schema manager and a connector.
// The schema manager ensures that all weaviate instances (or just the one) agree on which schema is used.
// The connector talks to the backing data store to persist the actual data.
type database struct {
	manager       SchemaManager
	connector     dbconnector.DatabaseConnector
	lockerKey     string
	lockerSession *concurrency.Session
}

type Params struct {
	// Messaging client, to be replaced with a more standardized logger like
	// logrus at some point. See gh-666.
	Messaging *messages.Messaging

	// The distributed RWLock is achieved using etcd. Instead of passing around a
	// reference to one RWMutex (which is impossible across multiple processes),
	// we pass around an etcd session that lives as long as this application as
	// well as a unique key. This key is used by the different processes
	// accessing etcd to know that they are requesting access to the same lock.
	LockerKey     string
	LockerSession *concurrency.Session

	// SchemaManager orchestrates schema changes. All those changes are applied
	// using a SchemaLock (which is the Write part on the RWMutex). This means
	// schema changes and "regular" changes, such as reading a class, updating a
	// class (while keeping the same schema) never happen at the same time as
	// updating the schema itself.
	SchemaManager SchemaManager

	// Connector allows us flexibility with the used databses. Users of the
	// "Database" type are agnostic of the underlying connector. This could be a
	// connector for a graph database, like Janusgraph or a NoSQL db or a SQL db.
	Connector dbconnector.DatabaseConnector

	// Contextionary used for NLP-processing
	Contextionary contextionary.Contextionary
}

func (p *Params) validate() error {
	makeError := func(fieldName string) error {
		return fmt.Errorf("invalid params: field '%s' is required", fieldName)
	}

	if p.Messaging == nil {
		return makeError("Messaging")
	}

	if p.LockerKey == "" {
		return makeError("LockerKey")
	}

	if p.LockerSession == nil {
		return makeError("LockerSession")
	}

	if p.SchemaManager == nil {
		return makeError("SchemaManager")
	}

	if p.Connector == nil {
		return makeError("Connector")
	}

	if p.Contextionary == nil {
		return makeError("Contextionary")
	}

	return nil
}

// New Database with Params. See docs of "Params" for more details on the
// individual params.
func New(params *Params) (Database, error) {
	if err := params.validate(); err != nil {
		return nil, err
	}

	manager, connector, messaging := params.SchemaManager, params.Connector, params.Messaging
	lockerSession, lockerKey := params.LockerSession, params.LockerKey
	contextionary := params.Contextionary

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

	lock := recipe.NewRWMutex(lockerSession, lockerKey)
	// Init the database. Manually lock the schema, so that initialization happens atomically across all instances.
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("could not get db (schema) lock: %s", err)
	}

	defer func() {
		err := lock.Unlock()
		if err != nil {
			log.Fatal(err)
		}
	}()
	errInit := connector.Init()
	if errInit != nil {
		messaging.ExitError(1, fmt.Sprintf("Could not initialize connector: %s", errInit.Error()))
	}

	manager.SetContextionary(contextionary)

	return &database{
		lockerKey:     lockerKey,
		lockerSession: lockerSession,
		manager:       manager,
		connector:     connector,
	}, nil
}

// Get a lock on the connector, allow access to the database, cannot modify schema.
func (db *database) ConnectorLock() (ConnectorLock, error) {
	lock := recipe.NewRWMutex(db.lockerSession, db.lockerKey)
	// Init the database. Manually lock the schema, so that initialization happens atomically across all instances.
	if err := lock.RLock(); err != nil {
		return nil, fmt.Errorf("could not get connector lock: %s", err)
	}

	return &connectorLock{
		db:     db,
		valid:  true,
		locker: lock,
	}, nil
}

// Get a lock on the schema manager. Can both modify the database and the schema.
// We ensure that only request in one instance should hold this lock at the same time.
func (db *database) SchemaLock() (SchemaLock, error) {
	lock := recipe.NewRWMutex(db.lockerSession, db.lockerKey)
	// Init the database. Manually lock the schema, so that initialization happens atomically across all instances.
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("could not get schema lock: %s", err)
	}

	return &schemaLock{
		db:     db,
		valid:  true,
		locker: lock,
	}, nil
}

func (db *database) GetResolver() (graphqlapi.ClosingResolver, error) {
	lock, err := db.ConnectorLock()
	if err != nil {
		return nil, err
	}

	return &dbClosingResolver{
		connectorLock: lock,
	}, nil
}
