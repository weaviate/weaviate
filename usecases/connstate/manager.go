//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package connstate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
)

// Manager can save and load a connector's internal state into a remote storage
type Manager struct {
	repo   Repo
	state  json.RawMessage
	logger logrus.FieldLogger
}

// Repo describes the dependencies of the connector state manager to an
// external storage
type Repo interface {
	Save(ctx context.Context, state json.RawMessage) error
	Load(ctx context.Context) (json.RawMessage, error)
}

// NewManager for Connector State
func NewManager(repo Repo, logger logrus.FieldLogger) (*Manager, error) {
	m := &Manager{repo: repo, logger: logger}
	if err := m.loadOrInitialize(context.Background()); err != nil {
		return nil, fmt.Errorf("could not load or initialize: %v", err)
	}

	return m, nil
}

// GetInitialState is only supposed to be used during initialization of the
// connector.
func (m *Manager) GetInitialState() json.RawMessage {
	return m.state
}

// SetState form outside (i.e. from the connector)
func (m *Manager) SetState(ctx context.Context, state json.RawMessage) error {
	m.state = state
	return m.save(ctx)
}

// func (l *etcdSchemaManager) SetStateConnector(stateConnector connector_state.Connector) {
// 	l.connectorStateSetter = stateConnector
// }

func (m *Manager) loadOrInitialize(ctx context.Context) error {
	state, err := m.repo.Load(ctx)
	if err != nil {
		return fmt.Errorf("could not load connector state: %v", err)
	}

	if state == nil {
		m.state = json.RawMessage([]byte("{}"))
		return m.save(ctx)
	}

	m.state = state
	return nil
}

func (m *Manager) save(ctx context.Context) error {
	m.logger.
		WithField("action", "connector_state_update").
		WithField("configuration_store", "etcd").
		Debug("saving updated connector state to configuration store")

	err := m.repo.Save(ctx, m.state)
	if err != nil {
		return fmt.Errorf("could not save connector state: %v", err)
	}

	return nil
}
