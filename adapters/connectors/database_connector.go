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

package connectors

import (
	"context"

	"github.com/sirupsen/logrus"

	graphqlapiLocal "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/usecases/kinds"
)

// BaseConnector is the interface that all connectors should have
type BaseConnector interface {
	// TODO: gh-836: restructure migrator and schema manager UCs
	schema_migrator.Migrator

	Connect() error
	Init(ctx context.Context) error
	SetServerAddress(serverAddress string)
	SetSchema(s schema.Schema)
	SetLogger(l logrus.FieldLogger)

	// kinds.Repo describes required methods to add, get, update and delete
	// things and actions
	kinds.Repo
}

// DatabaseConnector is the interface that all DB-connectors should have
type DatabaseConnector interface {
	BaseConnector
	connector_state.Connector
	graphqlapiLocal.Resolver
}
