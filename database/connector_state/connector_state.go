package connector_state

import (
	"encoding/json"
)

// Internal connector state
type StateManager interface {
	// Called by a connector when it has updated it's internal state that needs to be shared across all connectors in other Weaviate instances.
	SetState(state json.RawMessage) error

	// Used by a connector to get the initial state.
	GetState() json.RawMessage

	// Link a connector to this state manager.
	// When the internal state of some connector is updated, this state connector will call SetState on the provided conn.
	SetStateConnector(conn Connector)
}

// Implemented by a connector
type Connector interface {
	// Called by the state manager, when an external state update is happening.
	SetState(state json.RawMessage)

	// Link a StateManager to this connector, so that when a connector is updating it's own state, it can propagate these changes to othr Weaviate instances.
	SetStateManager(manager StateManager)
}
