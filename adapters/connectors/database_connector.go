/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package connectors

import (
	"context"
	"encoding/json"

	"github.com/sirupsen/logrus"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type StateManager interface {
	// Called by a connector when it has updated it's internal state that needs to be shared across all connectors in other Weaviate instances.
	SetState(ctx context.Context, state json.RawMessage) error

	// Used by a connector to get the initial state.
	GetInitialState() json.RawMessage

	// Link a connector to this state manager.
	// When the internal state of some connector is updated, this state connector will call SetState on the provided conn.
	// SetStateConnector(conn Connector)
}

type stateManagement interface {
	// Called by the state manager, when an external state update is happening.
	SetState(ctx context.Context, state json.RawMessage)

	// Link a StateManager to this connector, so that when a connector is updating it's own state, it can propagate these changes to othr Weaviate instances.
	SetStateManager(manager StateManager)
}

type initialization interface {
	Connect(ctx context.Context) error
	Init(ctx context.Context) error
	SetServerAddress(serverAddress string)
	SetSchema(s schema.Schema)
	SetLogger(l logrus.FieldLogger)
}

//DatabaseConnector is the interface that all connectors should have
type DatabaseConnector interface {
	// initialization methods that will be called to configure and init the
	// connector
	initialization

	// kinds.Repo describes required methods to add, get, update and delete
	// things and actions
	kinds.Repo

	// kinds.TraverserRepo describes required method to traverse the graph
	kinds.TraverserRepo

	// Migrator describes methods that will be called when the user changes the
	// schema. If the connected db is schemaless these have to return nil
	schemaUC.Migrator

	// stateManagement provdies the app with methods to update the state inside
	// the connector based on external events
	stateManagement
}
