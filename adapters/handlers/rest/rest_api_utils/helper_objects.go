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

// Package utils contains objects and functions designed to ease restapi operations
package rest_api_utils

import (
	"github.com/creativesoftwarefdn/weaviate/database"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/lib/delayed_unlock"
)

// A struct allowing the storage of tuples in a channel
type UnbatchedRequestResponse struct {
	RequestIndex int
	Response     *models.GraphQLResponse
}

// A struct supporting batched request handling by enabling appending both a request response and the request's index in the batch to a channel
type BatchedActionsCreateRequestResponse struct {
	RequestIndex int
	Response     *models.ActionsGetResponse
}

// A struct supporting batched request handling by enabling appending both a request response and the request's index in the batch to a channel
type BatchedThingsCreateRequestResponse struct {
	RequestIndex int
	Response     *models.ThingsGetResponse
}

// A struct supporting batched request handling by enabling easy transfer of dbconnector locks
type RequestLocks struct {
	DBLock      database.ConnectorLock
	DelayedLock delayed_unlock.DelayedUnlockable
	DBConnector dbconnector.DatabaseConnector
}
