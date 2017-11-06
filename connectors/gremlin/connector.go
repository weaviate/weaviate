/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package gremlin

import (
	errors_ "errors"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/gorilla/websocket"
	"github.com/mitchellh/mapstructure"

	"github.com/weaviate/weaviate/config"
	"github.com/weaviate/weaviate/connectors/utils"
	"github.com/weaviate/weaviate/messages"
	"github.com/weaviate/weaviate/models"
	"github.com/weaviate/weaviate/schema"
)

// Gremlin has some basic variables.
type Gremlin struct {
	client *websocket.Conn
	kind   string

	config        Config
	serverAddress string
	schema        *schema.WeaviateSchema
	messaging     *messages.Messaging
}

// Config represents the config outline for Gremlin. The Database config shoud be of the following form:
// "database_config" : {
//     "host": "127.0.0.1",
//     "port": 9080
// }
// Notice that the port is the GRPC-port.
type Config struct {
	Host string
	Port int
}

// GetName returns a unique connector name, this name is used to define the connector in the weaviate config
func (f *Gremlin) GetName() string {
	return "gremlin"
}

// SetConfig is used to fill in a custom struct with config variables, especially for Gremlin. These
// config variables are placed in the config file section "database_config" as follows:
//
// 	"database": {
// 		"name": "gremlin",
// 		"database_config" : {
// 			"host": "127.0.0.1",
// 			"port": 9080
// 		}
// 	},
func (f *Gremlin) SetConfig(configInput *config.Environment) error {
	err := mapstructure.Decode(configInput.Database.DatabaseConfig, &f.config)

	if err != nil || len(f.config.Host) == 0 || f.config.Port == 0 {
		return errors_.New("could not get Gremlin host/port from config")
	}

	return nil
}

// SetSchema is used to fill in a struct with schema. Custom actions are used.
func (f *Gremlin) SetSchema(schemaInput *schema.WeaviateSchema) error {
	f.schema = schemaInput

	return nil
}

// SetMessaging is used to fill the messaging object which is initialized in the server-startup.
func (f *Gremlin) SetMessaging(m *messages.Messaging) error {
	f.messaging = m

	return nil
}

// SetServerAddress is used to fill in a global variable with the server address, but can also be used
// to do some custom actions.
func (f *Gremlin) SetServerAddress(addr string) {
	f.serverAddress = addr
}

// Connect creates a connection to the database and tables if not already available.
// The connections could not be closed because it is used more often.
// runs before Init()
func (f *Gremlin) Connect() error {

	gremlinWsAddress := fmt.Sprintf("ws://%s:%d/gremlin", f.config.Host, f.config.Port)

	var dialer *websocket.Dialer

	clientConn, _, err := dialer.Dial(gremlinWsAddress, nil)
	if err != nil {
		return err
	}

	f.client = clientConn

	return nil
}

// Init creates a root key and initializes the schema in the database
// runs after Connect()
func (f *Gremlin) Init() error {

	f.messaging.InfoMessage("Initializing Gremlin...")

	return nil
}

// AddThing adds a thing to the Gremlin database with the given UUID.
func (f *Gremlin) AddThing(thing *models.Thing, UUID strfmt.UUID) error {

	println("ADD THING")

	return nil
}

// GetThing fills the given ThingGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetThing(UUID strfmt.UUID, thingResponse *models.ThingGetResponse) error {

	return nil
}

// ListThings fills the given ThingsListResponse with the values from the database, based on the given parameters.
func (f *Gremlin) ListThings(first int, offset int, keyID strfmt.UUID, wheres []*connutils.WhereQuery, thingsResponse *models.ThingsListResponse) error {

	return nil
}

// UpdateThing updates the Thing in the DB at the given UUID.
func (f *Gremlin) UpdateThing(thing *models.Thing, UUID strfmt.UUID) error {

	return nil
}

// DeleteThing deletes the Thing in the DB at the given UUID.
func (f *Gremlin) DeleteThing(UUID strfmt.UUID) error {

	return nil
}

// AddAction adds an Action to the Gremlin database with the given UUID
func (f *Gremlin) AddAction(action *models.Action, UUID strfmt.UUID) error {

	return nil
}

// GetAction fills the given ActionGettResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetAction(UUID strfmt.UUID, actionResponse *models.ActionGetResponse) error {

	return nil
}

// ListActions fills the given ActionsListResponse with the values from the database, based on the given parameters.
func (f *Gremlin) ListActions(UUID strfmt.UUID, first int, offset int, wheres []*connutils.WhereQuery, actionsResponse *models.ActionsListResponse) error {

	return nil
}

// UpdateAction updates a specific action based on the given UUID.
func (f *Gremlin) UpdateAction(action *models.Action, UUID strfmt.UUID) error {

	return nil
}

// DeleteAction deletes the Action in the DB at the given UUID.
func (f *Gremlin) DeleteAction(UUID strfmt.UUID) error {

	return nil
}

// AddKey adds a key to the Gremlin database with the given UUID and token.
func (f *Gremlin) AddKey(key *models.Key, UUID strfmt.UUID, token strfmt.UUID) error {

	return nil
}

// ValidateToken validates/gets a key to the Gremlin database with the given UUID
func (f *Gremlin) ValidateToken(UUID strfmt.UUID, key *models.KeyTokenGetResponse) error {

	println("VAL TOKEN")

	return nil
}

// GetKey fills the given KeyTokenGetResponse with the values from the database, based on the given UUID.
func (f *Gremlin) GetKey(UUID strfmt.UUID, keyResponse *models.KeyTokenGetResponse) error {

	println("GET KEY")

	return nil
}

// DeleteKey deletes the Key in the DB at the given UUID.
func (f *Gremlin) DeleteKey(UUID strfmt.UUID) error {

	return nil
}

// GetKeyChildren fills the given KeyTokenGetResponse array with the values from the database, based on the given UUID.
func (f *Gremlin) GetKeyChildren(UUID strfmt.UUID, children *[]*models.KeyTokenGetResponse) error {

	return nil
}
