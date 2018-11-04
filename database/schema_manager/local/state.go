package local

import (
	"encoding/json"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
)

// Only supposed to be used during initialization of the connector.
func (l *localSchemaManager) GetInitialConnectorState() json.RawMessage {
	return l.connectorState
}

func (l *localSchemaManager) SetState(state json.RawMessage) error {
	l.connectorState = state
	return l.saveConnectorStateToDisk()
}

func (l *localSchemaManager) SetStateConnector(stateConnector connector_state.Connector) {
	l.connectorStateSetter = stateConnector
}
