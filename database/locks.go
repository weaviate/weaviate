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
	"fmt"

	dbconnector "github.com/creativesoftwarefdn/weaviate/adapters/connectors"
	db_schema "github.com/creativesoftwarefdn/weaviate/entities/schema"
)

type RWLocker interface {
	Lock() error
	RLock() error

	Unlock() error
	RUnlock() error
}

type ConnectorLock interface {
	Connector() dbconnector.DatabaseConnector
	GetSchema() db_schema.Schema
	Unlock() error
}

type SchemaLock interface {
	ConnectorLock
	SchemaManager() SchemaManager
}

type connectorLock struct {
	db     *database
	valid  bool
	locker RWLocker
}

func (cl *connectorLock) Connector() dbconnector.DatabaseConnector {
	if cl.valid {
		return cl.db.connector
	} else {
		panic("this lock has already been released")
	}
}

func (cl *connectorLock) GetSchema() db_schema.Schema {
	if cl.valid {
		return cl.db.manager.GetSchema()
	} else {
		panic("this lock has already been released")
	}
}

func (cl *connectorLock) Unlock() error {
	err := cl.locker.RUnlock()
	if err != nil {
		return fmt.Errorf("could not unlock connector: %s", err)
	}

	cl.valid = false
	return nil
}

type schemaLock struct {
	db     *database
	valid  bool
	locker RWLocker
}

func (cl *schemaLock) GetSchema() db_schema.Schema {
	if cl.valid {
		return cl.db.manager.GetSchema()
	} else {
		panic("this lock has already been released")
	}
}

func (sl *schemaLock) Connector() dbconnector.DatabaseConnector {
	if sl.valid {
		return sl.db.connector
	} else {
		panic("this lock has already been released")
	}
}

func (sl *schemaLock) SchemaManager() SchemaManager {
	if sl.valid {
		return sl.db.manager
	} else {
		panic("this lock has already been released")
	}
}

func (sl *schemaLock) Unlock() error {
	err := sl.locker.Unlock()
	if err != nil {
		return fmt.Errorf("could not unlock schema: %s", err)
	}

	sl.valid = false
	return nil
}
