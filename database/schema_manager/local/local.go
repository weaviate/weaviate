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
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"time"

	libcontextionary "github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database"
	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/database/schema_migrator"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/creativesoftwarefdn/weaviate/network"
	"github.com/coreos/etcd/clientv3"
	log "github.com/sirupsen/logrus"
)

const SchemaStateStorageKey = "/weaviate/schema/state"
const ConnectorStateStorageKey = "/weaviate/connector/state"

type LocalSchemaConfig struct {
	StateDir *string `json:"state_dir"`
}

type localSchemaManager struct {
	// The directory where the state will be stored
	stateDir       string
	configStoreURL *url.URL

	// Persist schema
	stateFile   *os.File
	schemaState localSchemaState

	// Persist connector specific state.
	connectorStateFile   *os.File
	connectorState       json.RawMessage
	connectorStateSetter connector_state.Connector

	// Calling the migrator
	connectorMigrator schema_migrator.Migrator
	callbacks         []func(updatedSchema schema.Schema)

	// Contextionary
	contextionary libcontextionary.Contextionary

	// Network to validate cross-refs
	network network.Network
}

// The state that will be serialized to/from disk.
// TODO, see gh-477: refactor to database/schema_manager, so that it can be re-used for distributed version.
type localSchemaState struct {
	ActionSchema *models.SemanticSchema `json:"action"`
	ThingSchema  *models.SemanticSchema `json:"thing"`
}

func (l *localSchemaState) SchemaFor(k kind.Kind) *models.SemanticSchema {
	switch k {
	case kind.THING_KIND:
		return l.ThingSchema
	case kind.ACTION_KIND:
		return l.ActionSchema
	default:
		log.Fatalf("Passed wrong neither thing nor action, but %v", k)
		return nil
	}
}

func New(stateDirName string, configStoreURL *url.URL, connectorMigrator schema_migrator.Migrator, network network.Network) (database.SchemaManager, error) {
	manager := &localSchemaManager{
		configStoreURL:    configStoreURL,
		stateDir:          stateDirName,
		schemaState:       localSchemaState{},
		connectorMigrator: connectorMigrator,
		network:           network,
	}

	err := manager.loadOrInitializeSchema()
	if err != nil {
		return nil, err
	}

	err = manager.loadOrInitializeConnectorState()
	if err != nil {
		return nil, err
	}

	return manager, nil
}

// Load the state from a file, or if the files do not exist yet, initialize an empty schema.
func (l *localSchemaManager) loadOrInitializeSchema() error {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{l.configStoreURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("could not build etcd client: %s", err)
	}

	defer cli.Close()

	res, err := cli.Get(context.TODO(), SchemaStateStorageKey)
	if err != nil {
		return fmt.Errorf("could not retrieve key '%s': %#v", SchemaStateStorageKey, err)
	}

	switch k := len(res.Kvs); {
	case k == 0:
		// has not been initialized before
		l.schemaState.ActionSchema = &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
			Type:    "action",
		}

		l.schemaState.ThingSchema = &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{},
			Type:    "thing",
		}
		return l.saveToDisk()
	case k == 1:
		stateBytes := res.Kvs[0].Value
		var state localSchemaState
		err := json.Unmarshal(stateBytes, &state)
		if err != nil {
			return fmt.Errorf("Could not parse the schema state: %s", err)
		}
		l.schemaState = state
		return nil
	default:
		return fmt.Errorf("unexpected number of results for key '%s', expected to have 0 or 1, but got %d: %#v",
			SchemaStateStorageKey, len(res.Kvs), res.Kvs)
	}
}

// Load the
func (l *localSchemaManager) loadOrInitializeConnectorState() error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{l.configStoreURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("could not build etcd client: %s", err)
	}

	defer cli.Close()

	res, err := cli.Get(context.TODO(), ConnectorStateStorageKey)
	if err != nil {
		return fmt.Errorf("could not retrieve key '%s': %#v", ConnectorStateStorageKey, err)
	}

	switch k := len(res.Kvs); {
	case k == 0:
		// has not been initialized before
		l.connectorState = json.RawMessage([]byte("{}"))
		return l.saveConnectorStateToDisk()

	case k == 1:
		stateBytes := res.Kvs[0].Value
		return l.connectorState.UnmarshalJSON(stateBytes)
	default:
		return fmt.Errorf("unexpected number of results for key '%s', expected to have 0 or 1, but got %d: %#v",
			ConnectorStateStorageKey, len(res.Kvs), res.Kvs)
	}
}

// Save the schema to the local disk.
// Triggers callbacks to all interested observers.
func (l *localSchemaManager) saveToDisk() error {

	stateBytes, err := json.Marshal(l.schemaState)
	if err != nil {
		return fmt.Errorf("could not marshal schema state to json: %s", err)
	}

	log.Info("Updating local schema on etcd")

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{l.configStoreURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("could not build etcd client: %s", err)
	}

	defer cli.Close()

	_, err = cli.Put(context.TODO(), "/weaviate/schema/state", string(stateBytes))
	if err != nil {
		return fmt.Errorf("could not send schema state to etcd: %s", err)
	}

	l.TriggerSchemaUpdateCallbacks()

	return nil
}

// Save the connector state to disk.
// This local implementation has no side effects (like updating peer weaviate instances)
func (l *localSchemaManager) saveConnectorStateToDisk() error {

	stateBytes, err := l.connectorState.MarshalJSON()
	if err != nil {
		return fmt.Errorf("could not marshal connector state to json: %s", err)
	}

	log.Info("Saving connector state to etcd")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{l.configStoreURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("could not build etcd client: %s", err)
	}

	defer cli.Close()

	_, err = cli.Put(context.TODO(), "/weaviate/connector/state", string(stateBytes))
	if err != nil {
		return fmt.Errorf("could not send schema state to etcd: %s", err)
	}

	return nil
}
