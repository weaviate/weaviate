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
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/aggregate"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

type dbClosingResolver struct {
	connectorLock ConnectorLock
}

func (dbcr *dbClosingResolver) Close() {
	dbcr.connectorLock.Unlock()
}

func (dbcr *dbClosingResolver) LocalGetClass(info *get.Params) (interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	return connector.LocalGetClass(info)
}

func (dbcr *dbClosingResolver) LocalGetMeta(info *getmeta.Params) (interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	return connector.LocalGetMeta(info)
}

func (dbcr *dbClosingResolver) LocalAggregate(info *aggregate.Params) (interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	return connector.LocalAggregate(info)
}

func (dbcr *dbClosingResolver) LocalFetchKindClass(info *fetch.Params) (interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	return connector.LocalFetchKindClass(info)
}
