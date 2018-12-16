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
