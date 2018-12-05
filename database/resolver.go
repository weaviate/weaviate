package database

import (
	graphql_local_get      "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	graphql_local_get_meta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get_meta"
)

type dbClosingResolver struct {
	connectorLock ConnectorLock
}

func (dbcr *dbClosingResolver) Close() {
	dbcr.connectorLock.Unlock()
}

func (dbcr *dbClosingResolver) LocalGetClass(info *graphql_local_get.LocalGetClassParams) (func() interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	thunk, err := connector.LocalGetClass(info)
	return thunk, err
}

func (dbcr *dbClosingResolver) LocalGetMeta(info *graphql_local_get_meta.LocalGetMetaParams) (func() interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	thunk, err := connector.LocalGetMeta(info)
	return thunk, err
}
