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

// type dbClosingResolver struct {
// }

// func (dbcr *dbClosingResolver) Close() {
// 	// dbcr.connectorLock.Unlock()
// }

// func (dbcr *dbClosingResolver) LocalGetClass(info *kinds.LocalGetParams) (interface{}, error) {
// 	connector := dbcr.connectorLock.Connector()
// 	return connector.LocalGetClass(info)
// }

// func (dbcr *dbClosingResolver) LocalGetMeta(info *kinds.GetMetaParams) (interface{}, error) {
// 	connector := dbcr.connectorLock.Connector()
// 	return connector.LocalGetMeta(info)
// }

// func (dbcr *dbClosingResolver) LocalAggregate(info *aggregate.Params) (interface{}, error) {
// 	connector := dbcr.connectorLock.Connector()
// 	return connector.LocalAggregate(info)
// }

// func (dbcr *dbClosingResolver) LocalFetchKindClass(info *fetch.Params) (interface{}, error) {
// 	connector := dbcr.connectorLock.Connector()
// 	return connector.LocalFetchKindClass(info)
// }

// func (dbcr *dbClosingResolver) LocalFetchFuzzy(words []string) (interface{}, error) {
// 	connector := dbcr.connectorLock.Connector()
// 	return connector.LocalFetchFuzzy(words)
// }
