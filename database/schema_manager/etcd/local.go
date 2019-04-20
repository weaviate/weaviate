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
package etcd

// // SchemaStateStorageKey is the etcd key used to store the schema
// const SchemaStateStorageKey = "/weaviate/schema/state"

// // ConnectorStateStorageKey is the etcd key used to store the connector state
// const ConnectorStateStorageKey = "/weaviate/connector/state"

// type etcdClient interface {
// 	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
// 	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
// }

// type etcdSchemaManager struct {
// 	// etcd client to store and retrieve the schema and connector state
// 	client etcdClient

// 	// Persist schema
// 	schemaState state

// 	// Persist connector specific state.
// 	connectorState       json.RawMessage
// 	connectorStateSetter connector_state.Connector

// 	// Calling the migrator
// 	connectorMigrator schema_migrator.Migrator
// 	callbacks         []func(updatedSchema schema.Schema)

// 	// Contextionary
// 	contextionary libcontextionary.Contextionary

// 	// Network to validate cross-refs
// 	network network.Network

// 	logger logrus.FieldLogger
// }

// // The state that will be serialized to/from etcd.
// type state struct {
// 	ActionSchema *models.SemanticSchema `json:"action"`
// 	ThingSchema  *models.SemanticSchema `json:"thing"`
// }

// func (l *state) SchemaFor(k kind.Kind) *models.SemanticSchema {
// 	switch k {
// 	case kind.Thing:
// 		return l.ThingSchema
// 	case kind.Action:
// 		return l.ActionSchema
// 	default:
// 		// It is fine to panic here, as this indicates an unrecoverable error in
// 		// the program, rather than an invalid input based on user input
// 		panic(fmt.Sprintf("Passed wrong neither thing nor action, but %v", k))
// 	}
// }

// // New etcd schema manager which will save and read both the schema meta info
// // as well as the connector state (i.e. class name mappings) to and from etcd
// func New(ctx context.Context, client etcdClient, connectorMigrator schema_migrator.Migrator,
// 	network network.Network, logger logrus.FieldLogger) (database.SchemaManager, error) {
// 	manager := &etcdSchemaManager{
// 		client:            client,
// 		schemaState:       state{},
// 		connectorMigrator: connectorMigrator,
// 		network:           network,
// 		logger:            logger,
// 	}

// 	err := manager.loadOrInitializeConnectorState(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return manager, nil

// }

// // Load the
// func (m *etcdSchemaManager) loadOrInitializeConnectorState(ctx context.Context) error {
// 	res, err := m.client.Get(ctx, ConnectorStateStorageKey)
// 	if err != nil {
// 		return fmt.Errorf("could not retrieve key '%s': %#v", ConnectorStateStorageKey, err)
// 	}

// 	switch k := len(res.Kvs); {
// 	case k == 0:
// 		// has not been initialized before
// 		m.connectorState = json.RawMessage([]byte("{}"))
// 		return m.saveConnectorState(ctx)

// 	case k == 1:
// 		stateBytes := res.Kvs[0].Value
// 		return m.connectorState.UnmarshalJSON(stateBytes)
// 	default:
// 		return fmt.Errorf("unexpected number of results for key '%s', expected to have 0 or 1, but got %d: %#v",
// 			ConnectorStateStorageKey, len(res.Kvs), res.Kvs)
// 	}
// }

// // Save the connector state to disk.
// // This etcd implementation has no side effects (like updating peer weaviate instances)
// func (m *etcdSchemaManager) saveConnectorState(ctx context.Context) error {

// 	stateBytes, err := m.connectorState.MarshalJSON()
// 	if err != nil {
// 		return fmt.Errorf("could not marshal connector state to json: %s", err)
// 	}

// 	m.logger.
// 		WithField("action", "connector_state_update").
// 		WithField("configuration_store", "etcd").
// 		Debug("saving updated connector state to configuration store")

// 	_, err = m.client.Put(ctx, "/weaviate/connector/state", string(stateBytes))
// 	if err != nil {
// 		return fmt.Errorf("could not send schema state to etcd: %s", err)
// 	}

// 	return nil
// }
