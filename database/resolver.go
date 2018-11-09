package database
import (
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
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
