package database

import (
	dbconnector "github.com/creativesoftwarefdn/weaviate/connectors"
)

type RWLocker interface {
	Lock()
	RLock()

	Unlock()
	RUnlock()
}

type ConnectorLock struct {
	db    *Database
	valid bool
}

func (cl *ConnectorLock) Connector() *dbconnector.DatabaseConnector {
	if cl.valid {
		return cl.db.connector
	} else {
		panic("this lock has already been released")
	}
}

func (cl *ConnectorLock) GetSchema() Schema {
	if cl.valid {
		return (*cl.db.manager).GetSchema()
	} else {
		panic("this lock has already been released")
	}
}

func (cl *ConnectorLock) Unlock() {
	(*cl.db.locker).RUnlock()
	cl.valid = false
}

type SchemaLock struct {
	db    *Database
	valid bool
}

func (cl *SchemaLock) GetSchema() Schema {
	if cl.valid {
		return (*cl.db.manager).GetSchema()
	} else {
		panic("this lock has already been released")
	}
}

func (sl *SchemaLock) Connector() *dbconnector.DatabaseConnector {
	if sl.valid {
		return sl.db.connector
	} else {
		panic("this lock has already been released")
	}
}

func (sl *SchemaLock) SchemaManager() *SchemaManager {
	if sl.valid {
		return sl.db.manager
	} else {
		panic("this lock has already been released")
	}
}

func (sl *SchemaLock) Unlock() {
	(*sl.db.locker).Unlock()
	sl.valid = false
}
