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
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	db_schema "github.com/creativesoftwarefdn/weaviate/database/schema"
)

type RWLocker interface {
	Lock()
	RLock()

	Unlock()
	RUnlock()
}

type ConnectorLock interface {
	Connector() dbconnector.DatabaseConnector
	GetSchema() db_schema.Schema
	Unlock()
}

type SchemaLock interface {
	ConnectorLock
	SchemaManager() SchemaManager
}

type connectorLock struct {
	db    *database
	valid bool
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

func (cl *connectorLock) Unlock() {
	cl.db.locker.RUnlock()
	cl.valid = false
}

type schemaLock struct {
	db    *database
	valid bool
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

func (sl *schemaLock) Unlock() {
	sl.db.locker.Unlock()
	sl.valid = false
}
