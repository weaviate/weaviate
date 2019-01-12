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
	graphql_local_get "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	graphql_local_getmeta "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
)

type dbClosingResolver struct {
	connectorLock ConnectorLock
}

func (dbcr *dbClosingResolver) Close() {
	dbcr.connectorLock.Unlock()
}

func (dbcr *dbClosingResolver) LocalGetClass(info *graphql_local_get.LocalGetClassParams) (interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	thunk, err := connector.LocalGetClass(info)
	return thunk, err
}

func (dbcr *dbClosingResolver) LocalGetMeta(info *graphql_local_getmeta.LocalGetMetaParams) (interface{}, error) {
	connector := dbcr.connectorLock.Connector()
	return connector.LocalGetMeta(info)
}
