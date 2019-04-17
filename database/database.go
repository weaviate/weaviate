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
package database

import (
	"context"
	"fmt"
	"log"

	"github.com/coreos/etcd/clientv3/concurrency"
	dbconnector "github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql"
	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	recipe "github.com/etcd-io/etcd/contrib/recipes"
	"github.com/sirupsen/logrus"
)

type Database interface {
	graphql.DatabaseResolverProvider

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
	// Logrus logger
	Logger *logrus.Logger

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

	if p.Logger == nil {
		return makeError("Logger")
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
func New(ctx context.Context, params *Params) (Database, error) {
	if err := params.validate(); err != nil {
		return nil, err
	}

	manager, connector, logger := params.SchemaManager, params.Connector, params.Logger
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

	connector.SetState(ctx, initialState)
	connector.SetSchema(manager.GetSchema())
	connector.SetLogger(logger)

	// Make the connector try to connect to a database
	errConnect := connector.Connect()
	if errConnect != nil {
		logger.
			WithField("action", "database_init").
			WithError(errConnect).
			Error("could not establish connection in database connector")
		logger.Exit(1)
	}

	// Note that this lock is not passed to the returned &database{} type. Because
	// of the distributed nature of the locks (currently being managed with etcd)
	// the local types should not span the entire session. Each RWMutex holds the
	// state of the key it uses for locking. This means exactly this type has to
	// be reused for unlocking, but not used for anything else
	//
	// For example, the following is fine:
	//	m1 := recipe.NewRWMutex(/*...*/)
	//	m2 := recipe.NewRWMutex(/*...*/)
	//	m1.Lock()
	//	m1.Unlock()
	//	// now discard m1!
	//	m2.RLock()
	//	m2.RUnlock()
	//	// now discard m2!
	//
	// The above example might not be immediately intuitive, because when using
	// local mutexes it is extremly important to pass them by reference,
	// otherwise no syncing is possible. However on this remote lock it's exactly
	// the opposite.
	//
	// This code however would lead to problems:
	//	m := recipe.NewRWMutex(/*...*/)
	//
	//	go func() {
	// 		m.Lock()
	// 		// do something
	// 		m.Unlock()
	// 	}()
	//
	// 	go func() {
	// 		m.RLock()
	// 		// do something
	// 		m.RUnlock()
	// 	}()
	//
	// The above code segment would not work, because the first goroutine when
	// locking would set the internal m.myKey to whatever key and lease it got.
	// It is important that it keeps this state because it needs it to unlock the
	// same mutex, when doing m.Unlock(). However, the second goroutine would try
	// to do the same, it would set myKey to its own key+Lease on m.RLock(). This
	// means whichever goroutine of the two last called either Lock() or Unlock()
	// will have overwritten the internal key. Thus the other routine can never
	// be unlocked again, because on Unlock() or RUnlock() it would now try to
	// unlock with the other key.
	//
	// To fix the issue in Example 2, there should be no such thing as a "global"
	// Mutex, instead there should either be one per gouroutine or even better
	// yet, the mutexes should be created within the goroutines themselves.
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
	errInit := connector.Init(ctx)
	if errInit != nil {
		logger.
			WithField("action", "database_init").
			WithError(errInit).
			Error("could not initialize database connector")
		logger.Exit(1)
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
	// Note that we are creating a new lock every time that ConnectorLock() is
	// called. This lock has a short lifespan. It's only other usage, other than
	// calling RLock() in the next line is to pass it to the user, so that they
	// can call RUnlock() exactly once on it. This might seem counter-intuitive,
	// as with local locks we need to make sure there is only one global lock.
	// Please see database.New() for extensive documentation on why that would be
	// problematic in the case of the distributed lock.
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
	// Note that we are creating a new lock every time that SchemaLock() is
	// called. This lock has a short lifespan. It's only other usage, other than
	// calling Lock() in the next line is to pass it to the user, so that they
	// can call Unlock() exactly once on it. This might seem counter-intuitive,
	// as with local locks we need to make sure there is only one global lock.
	// Please see database.New() for extensive documentation on why that would be
	// problematic in the case of the distributed lock.
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

func (db *database) GetResolver() (graphql.ClosingResolver, error) {
	lock, err := db.ConnectorLock()
	if err != nil {
		return nil, err
	}

	return &dbClosingResolver{
		connectorLock: lock,
	}, nil
}
