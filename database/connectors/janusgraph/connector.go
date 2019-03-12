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
package janusgraph

import (
	"context"
	errors_ "errors"
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/connector_state"
	dbconnector "github.com/creativesoftwarefdn/weaviate/database/connectors"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/messages"
	"github.com/mitchellh/mapstructure"

	"github.com/creativesoftwarefdn/weaviate/gremlin/http_client"

	"github.com/sirupsen/logrus"
)

// Janusgraph has some basic variables.
// This is mandatory, only change it if you need aditional, global variables
type Janusgraph struct {
	client *http_client.Client
	kind   string

	initialized  bool
	stateManager connector_state.StateManager

	state state.JanusGraphConnectorState

	config        Config
	serverAddress string
	schema        schema.Schema
	messaging     *messages.Messaging
}

// Config represents the config outline for Janusgraph. The Database config shoud be of the following form:
// "database_config" : {
//     "Url": "http://127.0.0.1:8182"
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Url string
}

func New(config interface{}) (error, dbconnector.DatabaseConnector) {
	j := &Janusgraph{}
	err := j.setConfig(config)

	if err != nil {
		return err, nil
	} else {
		return nil, j
	}
}

// setConfig sets variables, which can be placed in the config file section "database_config: {}"
// can be custom for any connector, in the example below there is only host and port available.
//
// Important to bear in mind;
// 1. You need to add these to the struct Config in this document.
// 2. They will become available via f.config.[variable-name]
//
// 	"database": {
// 		"name": "janusgraph",
// 		"database_config" : {
// 			"url": "http://127.0.0.1:8081"
// 		}
// 	},
func (f *Janusgraph) setConfig(config interface{}) error {

	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(config, &f.config)
	if err != nil || len(f.config.Url) == 0 {
		return errors_.New("could not get Janusgraph url from config")
	}

	// If success return nil, otherwise return the error (see above)
	return nil
}

// SetSchema takes actionSchema and thingsSchema as an input and makes them available globally at f.schema
func (f *Janusgraph) SetSchema(schemaInput schema.Schema) {
	f.schema = schemaInput
}

// SetMessaging is used to send messages to the service.
// Available message types are: f.messaging.Infomessage ...DebugMessage ...ErrorMessage ...ExitError (also exits the service) ...InfoMessage
func (f *Janusgraph) SetMessaging(m *messages.Messaging) {

	// mandatory, adds the message functions to f.messaging to make them globally accessible.
	f.messaging = m
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
// Does not return anything
func (f *Janusgraph) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Init 1st initializes the schema in the database and 2nd creates a root key.
func (f *Janusgraph) Init(ctx context.Context) error {
	f.messaging.DebugMessage("Initializeing JanusGraph")

	err := f.ensureBasicSchema(ctx)
	if err != nil {
		return err
	}

	f.initialized = true

	return nil
}

// Connect connects to the Janusgraph websocket
func (f *Janusgraph) Connect() error {
	f.client = http_client.NewClient(f.config.Url)
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	f.client.SetLogger(logger)

	err := f.client.Ping()
	if err != nil {
		return fmt.Errorf("Could not connect to Gremlin server; %v", err)
	}

	f.messaging.InfoMessage("Sucessfully pinged Gremlin server")

	return nil
}

// Link a connector to this state manager.
// When the internal state of some connector is updated, this state connector will call SetState on the provided conn.
func (j *Janusgraph) SetStateManager(manager connector_state.StateManager) {
	j.stateManager = manager
}
